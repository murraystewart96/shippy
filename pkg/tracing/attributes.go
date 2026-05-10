package tracing

import "go.opentelemetry.io/otel/attribute"

// Common attribute keys for blockchain events
const (
	// Blockchain attributes
	AttrBlockNumber    = "blockchain.block_number"
	AttrTxHash         = "blockchain.tx_hash"
	AttrTxIndex        = "blockchain.tx_index"
	AttrEventType      = "blockchain.event_type"
	AttrContractAddr   = "blockchain.contract_address"
	
	// Token swap attributes  
	AttrTokenIn        = "swap.token_in"
	AttrTokenOut       = "swap.token_out"
	AttrAmountIn       = "swap.amount_in"
	AttrAmountOut      = "swap.amount_out"
	AttrPoolAddress    = "swap.pool_address"
	
	// Kafka attributes
	AttrKafkaTopic     = "kafka.topic"
	AttrKafkaPartition = "kafka.partition"
	AttrKafkaOffset    = "kafka.offset"
	
	// Database attributes
	AttrDBTable        = "db.table"
	AttrDBOperation    = "db.operation"
	
	// Cache attributes
	AttrCacheKey       = "cache.key"
	AttrCacheType      = "cache.type"
)

// Common attribute helper functions
func BlockchainAttributes(blockNumber uint64, txHash string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64(AttrBlockNumber, int64(blockNumber)),
		attribute.String(AttrTxHash, txHash),
	}
}

func SwapAttributes(tokenIn, tokenOut, amountIn, amountOut string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrTokenIn, tokenIn),
		attribute.String(AttrTokenOut, tokenOut),
		attribute.String(AttrAmountIn, amountIn),
		attribute.String(AttrAmountOut, amountOut),
	}
}

func KafkaAttributes(topic string, partition int32, offset int64) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrKafkaTopic, topic),
		attribute.Int(AttrKafkaPartition, int(partition)),
		attribute.Int64(AttrKafkaOffset, offset),
	}
}