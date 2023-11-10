# workshop_03

Este taller s basa en el entreno de un modelo de aprendizaje automatico de regresion para prececir el happiness_score (puntuacion de felicidad) a partir 5 csv que contienen informacion sobre la puntuacion de felicidad en diferentes paises. Para ello el modelo usa una division de datos de 70% para entrenamiento y 30% de pruebas. A su vez se transmitiran los datos transformados mediante kafka para el el consumer haga la prediccion del puntaje.

## Requisitos

Antes de comenzar asegurate de tener instalados los isguientes componentes:

- Python, lo mas recomendable es que sea la ultima version.
- Docker, puedes hacer uso de docker desktop o descargarlo enl amaquina que dispongas.
- Jupyter, en caso de que quieras tener los notebooks.

## Configuracion de Postgresql 
1. Descarga e Instalación: Ve al sitio web oficial de PostgreSQL y descarga la versión adecuada para tu sistema operativo.
2. Configuración: Durante la instalación, se te pedirá establecer una contraseña para el usuario predeterminado postgres.
3. Herramientas Gráficas (opcional): Puedes instalar herramientas gráficas como pgAdmin para gestionar y trabajar con tus bases de datos de PostgreSQL de manera visual.

### Configuracion Postgresql con python
1. Instala el paquete psycopg2: En tu entorno de Python, instala el paquete psycopg2 que permite la conexión con PostgreSQL. Puedes hacerlo utilizando pip:
 ```bash
pip install psycopg2

2. Conexión a la base de datos:
