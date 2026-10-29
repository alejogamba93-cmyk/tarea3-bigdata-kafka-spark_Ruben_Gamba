# Tarea 3: Procesamiento de Datos en Tiempo Real y por Lotes (Batch y Streaming)

## Descripción del Proyecto
Este repositorio contiene el desarrollo práctico de la Tarea 3 del curso de **Big Data** de la UNAD. El proyecto implementa una arquitectura distribuida utilizando **Apache Spark**, **Spark SQL**, **PySpark**, **Apache Kafka** y **Spark Structured Streaming** sobre un entorno virtualizado en Ubuntu.

## Objetivos del Proyecto
* **Módulo Batch:** Realizar la analítica de datos históricos de Infracciones de Tránsito, aplicando limpieza de datos, columnas derivadas, consultas con Spark SQL y agregaciones distribuidas con RDDs.
* **Módulo Streaming:** Implementar una arquitectura en tiempo real para el monitoreo continuo de transacciones financieras, utilizando un productor en Python y un consumidor con Spark Structured Streaming conectado a un clóster de Apache Kafka.
* **Validación de Rendimiento:** Monitorear y administrar los recursos del clúster a través de la Spark Web UI (Puerto 4040).

## Tecnologías Utilizadas
* Python / PySpark 3.5
* Apache Kafka 3.5.1
* Apache Spark (Spark SQL y Structured Streaming)
* Ubuntu Linux (Máquina Virtual en VirtualBox)
