package main

import (
	"log"
	"math/rand"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

//Funció per imprimir l'e si n'hi ha hagut
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

	//Declaram la coa a través de la cual les abelles despertaràn a l'os
	coaAvisos, e := canal.QueueDeclare(
		"avisos", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(e, "Error al declarar la coa dels avisos.")

	//Declaram la coa a través de la cual les abelles consumiran permisos enviats per l'os
	perms, e := canal.QueueDeclare(
		"permisos",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(e, "Error al declarar la coa dels permisos.")

	e = canal.Qos(
		1,
		0,
		false,
	)
	failOnError(e, "Failed to set QoS")

	missatges, e := canal.Consume(
		perms.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(e, "Error a l'intentar registrar un consumidor.")

	//Declaram l'Exchange pel qual l'os enviarà el missatge d'acabar a les abelles
	e = canal.ExchangeDeclare(
		"final",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(e, "No s'ha pogut declarar l'Exchange")

	local, e := canal.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	failOnError(e, "Error al declarar una coa")
	e = canal.QueueBind(
		local.Name,
		"",
		"final",
		false,
		nil)
	failOnError(e, "Error a l'enllaçar la coa")
	msgFinal, e := canal.Consume(
		local.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	nomAbella := os.Args[0]
	forever := make(chan bool)
	go func() {
		for missatge := range missatges {
			if missatge.RoutingKey == perms.Name {
				log.Printf("L'abella %s produeix la mel nombre %s", nomAbella, missatge.Body)
				missatge.Ack(false)
				//Si és la abella que posa la mel 10, despertam a l'os
				if string(missatge.Body[:]) == "10" {

					e = canal.Publish(
						"",
						coaAvisos.Name,
						false,
						false,
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "text/plain",
							Body:         []byte(nomAbella),
						})
					failOnError(e, "Error a l'enviar a l'os el missatge de despertar.")
					log.Printf("Som l'abella %s i despert a l'os", nomAbella)
				}
				time.Sleep(time.Duration(1 + rand.Float64()*2))
			}
		}
	}()

	go func() {
		for msg := range msgFinal {
			if msg.RoutingKey != perms.Name {
				msg.Ack(false)
				log.Printf("El pot de mel s'ha romput!")
				log.Printf("La abella %s torna al rusc.")
				log.Printf("Simulació acabada.")
				os.Exit(0)
			}
		}
	}()
	log.Printf(" Esperant missatges... ")
	<-forever
}
