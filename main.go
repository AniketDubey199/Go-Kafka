package main

import (
	"log"

	"github.com/AniketDubey199/Go-Kafka/config"
	"github.com/AniketDubey199/Go-Kafka/producer"
	"github.com/gofiber/fiber/v3"
)

func main() {
	broker := []string{"localhost:9092"}
	p, err := producer.ConnectProducers(broker)
	if err != nil {
		panic(err)
	}

	config.Producers = p
	defer config.Producers.Close()

	app := fiber.New()
	api := app.Group("/api")
	producer.PlaceOrder(api)
	log.Fatal(app.Listen(":3000"))
}
