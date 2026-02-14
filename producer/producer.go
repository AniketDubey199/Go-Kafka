package producer

import (
	"encoding/json"
	"log"

	con "github.com/AniketDubey199/Go-Kafka/config"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v3"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeeType   string `json:"coffee_type"`
}

func ConnectProducers(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func PushOrderToQueue(topic string, message []byte) error {
	// connection main me karna hai aur ek hi baar call karenge jaise app start hoga

	// create message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := con.Producers.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic,
		partition,
		offset)

	return nil
}

func PlaceOrder(route fiber.Router) {
	route.Post("/order", func(c fiber.Ctx) error {

		// `1. Parse body into object
		order := new(Order)
		if err := c.Bind().Body(order); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		// 2. Convert it into bytes
		orderbytes, err := json.Marshal(order)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		// 3.Send the bytes to kafka
		err = PushOrderToQueue("coffee_order", orderbytes)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		// 4.Response back to the user
		response := map[string]interface{}{
			"success": true,
			"Msg":     "Order for " + order.CustomerName + "placed successfully",
		}

		return c.Status(fiber.StatusOK).JSON(response)
	})
}
