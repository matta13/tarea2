-En primer lugar, se debe clonar el repositorio.

-En segundo lugar, se accede desde cmd al directorio donde se encuentra la carpeta.

-paso 3, construir y levantar contenedores con el comando: docker compose up --build -d.

-paso 4, escribir el siguiente comando para ingresar al contenedor del cliente: docker exec -it qa_client sh.

-paso 5, escribir el comando: python3 client.py para ejecutar el codigo del cliente y aparecera un espacio para escribir preguntas.

-La aplicacion ya esta lista para realizar consultas tanto de las que se encuentran en la base como otras preguntas, en caso de no estar la pregunta en la base esta se envia a kafka, la pregunta se copia en el topico querys, desde el cual la consume el LLM este produce una respuesta y la copia en el topico llm_answers, luego el scorer consume la pregunta desde ahi, mediante la api de gemini genera un puntaje en base a los criterios entregados y le asigna un puntaje el cual se produce en el topico final_answer y por ultimo de ahi lo consume db_writer el cual agrega la pregunte, junto a su respuesta y su puntaje a la base de datos.
