package integration

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"testing"

	"github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/server"
	"github.com/murraystewart96/shippy/consignment-service/storage/mongo"
	"github.com/murraystewart96/shippy/pkg/kafka"
	consignmentpb "github.com/murraystewart96/shippy/proto/consignment"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcMongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	mongoDriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var s *suite

type suite struct {
	kafkaAddr       string
	mongoCli        *mongoDriver.Client
	producer        kafka.IProducer
	paymentSvc      *mockPaymentService
	paymentAddr     string
	reservationSvc  *mockReservationService
	consignmentAddr string
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

	// --- MongoDB ---
	mongoContainer, err := tcMongo.Run(ctx, "mongo:7", tcMongo.WithReplicaSet("rs0"))
	if err != nil {
		panic(err)
	}
	defer mongoContainer.Terminate(ctx)

	mongoURI, err := mongoContainer.ConnectionString(ctx)
	if err != nil {
		panic(err)
	}
	// directConnection bypasses replica-set member discovery so the driver
	// uses the exposed Docker port rather than the container's internal IP,
	// which is unreachable from the host on macOS.
	mongoURI += "&directConnection=true"

	mongoCli, err := mongoDriver.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(err)
	}
	defer mongoCli.Disconnect(ctx)

	// --- Shared producer ---
	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		BootstrapServers: kafkaAddr,
		Acks:             "all",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// --- In-process mock payment gRPC server ---
	paymentSvc := &mockPaymentService{}
	paymentLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	paymentSrv := grpc.NewServer()
	paymentpb.RegisterPaymentServiceServer(paymentSrv, paymentSvc)
	go func() { _ = paymentSrv.Serve(paymentLis) }()
	defer paymentSrv.Stop()

	// --- In-process mock reservation gRPC server ---
	reservationSvc := &mockReservationService{}
	reservationLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	reservationSrv := grpc.NewServer()
	reservepb.RegisterReservationServiceServer(reservationSrv, reservationSvc)
	go func() { _ = reservationSrv.Serve(reservationLis) }()
	defer reservationSrv.Stop()

	// --- Ensure Kafka topics ---
	if err := kafka.EnsureTopics(ctx, kafkaAddr, []kafka.TopicConfig{
		{Name: manager.ConsignmentPaymentAuthorisedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ConsignmentConfirmationFailedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ReservationExpiredTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.PaymentCapturedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ReservationConfirmedTopic, NumPartitions: 1, ReplicationFactor: 1},
	}); err != nil {
		panic(err)
	}

	// --- In-process CS gRPC server ---
	paymentConn, err := grpc.NewClient(paymentLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer paymentConn.Close()

	reservationConn, err := grpc.NewClient(reservationLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer reservationConn.Close()

	consignmentCol := mongoCli.Database("shippy_test").Collection("consignments")
	outboxCol := mongoCli.Database("shippy_test").Collection("outbox")

	h := server.NewHandler(
		mongo.New(consignmentCol),
		reservepb.NewReservationServiceClient(reservationConn),
		paymentpb.NewPaymentServiceClient(paymentConn),
		mongo.NewOutbox(outboxCol),
	)
	consignmentSrv := server.NewGRPCServer(h)
	consignmentLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() { _ = consignmentSrv.Serve(consignmentLis) }()
	defer consignmentSrv.Stop()

	s = &suite{
		kafkaAddr:       kafkaAddr,
		mongoCli:        mongoCli,
		producer:        producer,
		paymentSvc:      paymentSvc,
		paymentAddr:     paymentLis.Addr().String(),
		reservationSvc:  reservationSvc,
		consignmentAddr: consignmentLis.Addr().String(),
	}

	os.Exit(m.Run())
}

// cleanState drops the test collections between tests to prevent state leaking.
func (s *suite) cleanState(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		db := s.mongoCli.Database("shippy_test")
		_ = db.Collection("consignments").Drop(context.Background())
		_ = db.Collection("outbox").Drop(context.Background())
	})
}

// newManager creates a real manager wired to the test infrastructure.
func (s *suite) newManager(t *testing.T, topics []string) *manager.Manager {
	t.Helper()

	paymentConn, err := grpc.NewClient(s.paymentAddr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { _ = paymentConn.Close() })

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: s.kafkaAddr,
		GroupID:          "integration-test-consignment-manager",
		OffsetReset:      "earliest",
	})
	require.NoError(t, err)

	outbox := mongo.NewOutbox(s.mongoCli.Database("shippy_test").Collection("outbox"))
	repository := mongo.New(s.mongoCli.Database("shippy_test").Collection("consignments"))
	store := mongo.NewStore(s.mongoCli)

	mgr, err := manager.New(
		s.producer,
		consumer,
		topics,
		outbox,
		store,
		paymentpb.NewPaymentServiceClient(paymentConn),
		nil,
		repository,
		manager.Config{OutboxInterval: 1},
	)
	require.NoError(t, err)

	return mgr
}

// newConsignmentClient returns a gRPC client connected to the in-process CS server.
func (s *suite) newConsignmentClient(t *testing.T) consignmentpb.ConsignmentServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(s.consignmentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return consignmentpb.NewConsignmentServiceClient(conn)
}

// publish sends a message directly to a Kafka topic.
func (s *suite) publish(t *testing.T, topic, key string, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, s.producer.Produce(context.Background(), topic, []byte(key), payload, nil))
}
