/*
	- Jonathan Salisbury Vega
	- Joan Sanso Pericas
	Video: https://youtu.be/s-hPI3O7UxY

*/

package main

import (
	"log"
	"math/rand"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

//Funció per imprimir l'error si n'hi ha hagut
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
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

	//Declaram l'Exchange pel qual l'os enviarà el missatge d'acabar a les abelles
	//que es de tipus fanout
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

	//Aquesta és una coa local que anirà associada a l'Exchange
	local, e := canal.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	failOnError(e, "Error al declarar una coa")

	//Associam la coa local a l'exchange "final".
	e = canal.QueueBind(
		local.Name,
		"",
		"final",
		false,
		nil)
	failOnError(e, "Error a l'enllaçar la coa")

	//Consumició dels missatges: consumim de la coa de permisos
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

	//Consumim de la coa local que contendrà el missatge de acabament.
	msgFinal, e := canal.Consume(
		local.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	nomAbella := os.Args[1] //Agafam el nom de l'abella dels arguments de consola

	forever := make(chan bool)
	//Aquesta gorutina s'encarregarà de llegir els missatges de la coa de permisos, i publicarà, en cas de ser la abella número 10,
	// un missatge a la coa dels avisos per avisar a l'os de que el pot ja està ple.
	go func() {
		for missatge := range missatges {
			//Si el missatge prové de la coa de permisos
			if missatge.RoutingKey == perms.Name {
				//Produim la mel
				log.Printf("L'abella %s produeix la mel nombre %s", nomAbella, missatge.Body)
				missatge.Ack(false)
				//Si és la abella que posa la mel 10, despertam a l'os
				if string(missatge.Body[:]) == "10" {
					//Publicam el missatge per la coa dels avisos, amb el nom de l'abella com a cos del missatge, així l'os
					// pot saber quina abella l'ha despertat.
					log.Printf("Som l'abella %s i despert a l'os", nomAbella)
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
				}
				//Feim un sleep per forçar l'intercalat
				time.Sleep(time.Duration(rand.Float64()*3) * time.Second)
			}
		}
	}()

	//Aquesta gorutina analitza els missatges de la coa de finalització
	go func() {
		for msg := range msgFinal {
			//Si el missatge no prové de la coa de permisos, és que prové de la coa local, i acabam la simulació
			if msg.RoutingKey != perms.Name {
				msg.Ack(false)
				log.Printf("El pot de mel s'ha romput!")
				log.Printf("La abella %s torna al rusc.", nomAbella)
				log.Printf("Simulació acabada.")
				os.Exit(0)
			}
		}
	}()
	log.Printf(" Esperant missatges... ")
	<-forever
}
