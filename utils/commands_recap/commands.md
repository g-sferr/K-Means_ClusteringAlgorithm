
## Short Command Recap

Remeber to roperly set the "config.properties" file before launch the application.

#### compile command on hadoop

```shell
mvn clean package
```

#### run command on hadoop

```shell
hadoop jar target/KMeans-1.0-SNAPSHOT.jar it.unipi.dii.cc.hadoop.Kmeans <input_file_name> <output_folder_name> <number_of_reducers>
```