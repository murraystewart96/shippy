package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcRedis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/murraystewart96/shippy/pkg/kafka"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/manager"
	postgrespkg "github.com/murraystewart96/shippy/reservation-service/storage/postgres"
	redispkg "github.com/murraystewart96/shippy/reservation-service/storage/redis"
)

var s *suite

type suite struct {
	kafkaAddr  string
	pgDB       *postgrespkg.DB
	rdb        *goredis.Client
	cache      *redispkg.Cache
	producer   kafka.IProducer
	vesselSvc  *mockVesselService
	vesselAddr string
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	// --- Kafka ---
	kafkaContainer, err := tcKafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		panic(err)
	}
	defer kafkaContainer.Terminate(ctx)

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		panic(err)
	}
	kafkaAddr := brokers[0]

	// --- Postgres ---
	pgContainer, err := tcPostgres.Run(ctx, "postgres:16",
		tcPostgres.WithDatabase("shippy_test"),
		tcPostgres.WithUsername("test"),
		tcPostgres.WithPassword("test"),
		tcPostgres.WithInitScripts("../database/sql/init.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	if err != nil {
		panic(err)
	}
	defer pgContainer.Terminate(ctx)

	pgHost, err := pgContainer.Host(ctx)
	if err != nil {
		panic(err)
	}
	pgPort, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		panic(err)
	}

	pgDB, err := postgrespkg.NewDB(&config.DB{
		Host:     pgHost,
		Port:     pgPort.Port(),
		Name:     "shippy_test",
		User:     "test",
		Password: "test",
	})
	if err != nil {
		panic(err)
	}

	// --- Redis ---
	redisContainer, err := tcRedis.Run(ctx, "redis:7")
	if err != nil {
		panic(err)
	}
	defer redisContainer.Terminate(ctx)

	redisAddr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		panic(err)
	}
	// ConnectionString returns "redis://host:port" — strip the scheme for go-redis
	redisAddr = redisAddr[len("redis://"):]

	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	cache := redispkg.NewCache(&config.Redis{
		Addr:               redisAddr,
		ReservationTTL:     60,
		ReservationDataTTL: 300,
	})

	// --- Shared producer ---
	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		BootstrapServers: kafkaAddr,
		Acks:             "all",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// --- In-process mock vessel gRPC server ---
	vesselSvc := &mockVesselService{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	vesselSrv := grpc.NewServer()
	vesselpb.RegisterVesselServiceServer(vesselSrv, vesselSvc)
	go func() { _ = vesselSrv.Serve(lis) }()
	defer vesselSrv.Stop()

	// --- Ensure Kafka topics ---
	if err := kafka.EnsureTopics(ctx, kafkaAddr, []kafka.TopicConfig{
		{Name: manager.ReleaseCapacityTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ConfirmCapacityTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.CapacityFailedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.PaymentCapturedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ConsignmentConfirmationFailedTopic, NumPartitions: 1, ReplicationFactor: 1},
	}); err != nil {
		panic(err)
	}

	s = &suite{
		kafkaAddr:  kafkaAddr,
		pgDB:       pgDB,
		rdb:        rdb,
		cache:      cache,
		producer:   producer,
		vesselSvc:  vesselSvc,
		vesselAddr: lis.Addr().String(),
	}

	os.Exit(m.Run())
}

// cleanState truncates the outbox and flushes Redis between tests.
func (s *suite) cleanState(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		_, err := s.pgDB.GetConn().Exec(context.Background(), "TRUNCATE outbox")
		require.NoError(t, err)

		require.NoError(t, s.rdb.FlushDB(context.Background()).Err())
	})
}

// newManager creates a real manager wired to the test infrastructure.
func (s *suite) newManager(t *testing.T, topics []string) *manager.Manager {
	t.Helper()

	vesselConn, err := grpc.NewClient(s.vesselAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = vesselConn.Close() })

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: s.kafkaAddr,
		GroupID:          fmt.Sprintf("test-%d", time.Now().UnixNano()),
		OffsetReset:      "earliest",
	})
	require.NoError(t, err)

	mgr, err := manager.New(
		vesselpb.NewVesselServiceClient(vesselConn),
		s.producer,
		consumer,
		topics,
		s.cache,
		s.pgDB,
		config.Manager{OutboxInterval: 1, CleanupInterval: 3600},
	)
	require.NoError(t, err)

	return mgr
}

// publish sends a message directly to a Kafka topic.
func (s *suite) publish(t *testing.T, topic, key string, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, s.producer.Produce(context.Background(), topic, []byte(key), payload, nil))
}
