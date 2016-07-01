package kafka

import (
	"log"
	"sync"

	"github.com/bsm/sarama-cluster"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	"github.com/raintank/raintank-metric/metric_tank/in"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/usage"
)

type Kafka struct {
	in.In
	consumer *cluster.Consumer
	stats    met.Backend

	wg sync.WaitGroup
	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
}

func New(broker, topic string, stats met.Backend) *Kafka {
	brokers := []string{broker}
	groupId := "group1"
	topics := []string{topic}

	config := cluster.NewConfig()
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true
	err := config.Validate()
	if err != nil {
		log.Fatalln("invalid config", err)
	}
	consumer, err := cluster.NewConsumer(brokers, groupId, topics, config)
	if err != nil {
		log.Fatalln("failed to start consumer: ", err)
	}
	log.Println("consumer started without error")
	k := Kafka{
		consumer: consumer,
		stats:    stats,
		StopChan: make(chan int),
	}

	return &k
}

func (k *Kafka) Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) {
	k.In = in.New(metrics, defCache, usg, "kafka", k.stats)
	go k.notifications()
	go k.consume()
}

func (k *Kafka) consume() {
	k.wg.Add(1)
	messageChan := k.consumer.Messages()
	for msg := range messageChan {
		log.Printf("received message: Topic %s, Partition: %d, Offset: %d, Key: %s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
		k.In.Handle(msg.Value)
		k.consumer.MarkOffset(msg, "")
	}
	log.Println("kafka consumer ended.")
	k.wg.Done()
}

func (k *Kafka) notifications() {
	k.wg.Add(1)
	for msg := range k.consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Printf("consumer claimed %d partitions on topic: %s\n", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Printf("consumer released %d partitions on topic: %s\n", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Printf("consumer is no longer consuming from any partitions.")
		} else {
			log.Printf("Current partitions:\n")
			for topic, partitions := range msg.Current {
				log.Printf("%s: %v\n", topic, partitions)
			}
		}
	}
	log.Printf("kafka notification processing stopped")
	k.wg.Done()
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (k *Kafka) Stop() {
	// closes notifications and messages channels, amongst others
	k.consumer.Close()

	go func() {
		k.wg.Wait()
		close(k.StopChan)
	}()
}
