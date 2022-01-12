package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	//Cream la connexió a RabbitMQ
	connexio, e := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(e, "Connexió a RabbitMQ fallida.")
	defer connexio.Close()

	//Cream un canal
	canal, e := connexio.Channel()
	failOnError(e, "Error a l'obrir un canal.")
	defer canal.Close()

	//Declaram la coa a través l'os rebrà el missatge per despertarse
	coaAvisos, e := canal.QueueDeclare(
		"avisos", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(e, "Error al declarar la coa dels avisos.")

	//Declaram l'exchange on l'os enviarà el missatge de finalitzar a les
	// abelles.
	e = canal.ExchangeDeclare(
		"final",
		"fanout",
		true,
		false,
		false,
		false,
		nil)
	failOnError(e, "Error al declarar l'Exchange.")

	e = canal.Qos(
		1,
		0,
		false)
	failOnError(e, "Failed to set QoS")
	missatges, e := canal.Consume(
		coaAvisos.Name,
		"",
		false,
		false,
		false,
		false,
		nil)
	failOnError(e, "No s'ha pogut registrar el Consumer")

	//Declaram el canal de permisos (on l'os posarà els permisos)
	perms, e := canal.QueueDeclare(
		"permisos",
		true,
		false,
		false,
		false,
		nil)
	failOnError(e, "Error al declarar el canal de permisos.")

	for i := 1; i <= 10; i++ {
		permis := strconv.Itoa(i)

		e := canal.Publish(
			"",
			perms.Name,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(permis),
			})
		failOnError(e, "No s'ha publicat un permís.")
		log.Printf("Enviat %s", permis)
	}

	forever := make(chan bool)
	picsMenjat := 0
	go func() {
		for msg := range missatges {
			picsMenjat++
			log.Printf("L'abella %s m'ha despertat. Menj %d/3.", msg.Body, picsMenjat)
			time.Sleep(time.Duration(1+rand.Float64()*3) * time.Second)
			if picsMenjat == 3 {
				msg.Ack(false)
				finiquito := "acaba"
				log.Printf("L'os esta ple, ha romput el pot de mel!")
				e = canal.Publish(
					"final",
					"",
					false,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(finiquito)})
				failOnError(e, "El missatge de finalització no s'ha publicat.")
				os.Exit(0)
			} else {
				for i := 1; i <= 10; i++ {
					permis := strconv.Itoa(i)

					e := canal.Publish(
						"",
						perms.Name,
						false,
						false,
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "text/plain",
							Body:         []byte(permis),
						})
					failOnError(e, "No s'ha publicat un permís.")
					log.Printf("Enviat %s", permis)
				}
				msg.Ack(false)
			}
		}
	}()
	log.Printf(" Esperant missatges... ")
	<-forever
}
