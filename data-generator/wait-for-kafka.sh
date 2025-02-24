#!/bin/sh
echo " Attente de Kafka..."
while ! nc -z kafka 9092; do
  sleep 2
done
echo " Kafka est prêt, lancement du data-generator !"
