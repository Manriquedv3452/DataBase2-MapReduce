run:
	javac -classpath '/home/manriquedv/Descargas/mysql-connector-java-5.1.42/mysql-connector-java-5.1.42-bin.jar'   -d 'insert_classes/' 'InsertMySQL.java'

	jar -cvf insert.jar -C 'insert_classes' .

	java -cp insert.jar:'/home/manriquedv/Descargas/mysql-connector-java-5.1.42/mysql-connector-java-5.1.42-bin.jar'  InsertMySQL

