package com.cassandra.dataloader.cassandraloader;

import com.cassandra.dataloader.cassandraloader.Books.Book;
import com.cassandra.dataloader.cassandraloader.Books.BookRepository;
import com.cassandra.dataloader.cassandraloader.author.Author;
import com.cassandra.dataloader.cassandraloader.author.AuthorRepository;
import com.cassandra.dataloader.cassandraloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class CassandraLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(CassandraLoaderApplication.class, args);
    }

    private void initAuthors() {

        Path path = Paths.get(authorDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                //Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));

                JSONObject jsonObject;
                try {
                    jsonObject = new JSONObject(jsonString);
                    //Construct Author object
                    Author author = Author.builder()
                            .name(jsonObject.optString("name"))
                            .id(jsonObject.optString("key").replace("/authors/", ""))
                            .personalName(jsonObject.optString("personal_name")).build();


                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    private void initWorks() {

        Path path = Paths.get(worksDumpLocation);

        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

        try (Stream<String> lines = Files.lines(path)) {
            lines.limit(50).forEach(line -> {
                //Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));

                JSONObject jsonObject;
                try {
                    jsonObject = new JSONObject(jsonString);
                    //Construct Book object

                    String booksdescription = null;
                    JSONObject descriptionObject = jsonObject.optJSONObject("description");
                    if (descriptionObject != null)
                        booksdescription = descriptionObject.optString("value");


                    LocalDate publishedDate = null;
                    JSONObject publishedObj = jsonObject.optJSONObject("created");
                    if (publishedObj != null) {
                        String dateString = publishedObj.getString("value");
                        publishedDate = LocalDate.parse(dateString, dateFormat);
                    }

                    List<String> coverIdsList = new ArrayList<>();
                    JSONArray coversJSONArray = jsonObject.optJSONArray("covers");
                    if (coversJSONArray != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversJSONArray.length(); i++) {
                            coverIds.add(coversJSONArray.getString(i));
                        }
                        coverIdsList.addAll(coverIds);
                    }

                    List<String> authorIdsList = new ArrayList<>();
                    JSONArray authorsJSONArray = jsonObject.optJSONArray("authors");
                    if (coversJSONArray != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < coversJSONArray.length(); i++) {
                            String authorId = authorsJSONArray
                                    .getJSONObject(i)
                                    .getJSONObject("author")
                                    .getString("key")
                                    .replace("/authors/", "");

                            authorIds.add(authorId);
                        }
                        authorIdsList.addAll(authorIds);


                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (!optionalAuthor.isPresent()) return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());


                        Book book = Book.builder()
                                .id(jsonObject.optString("key").replace("/works/", ""))
                                .name(jsonObject.optString("title"))
                                .description(booksdescription)
                                .publishedDate(publishedDate)
                                .coverIds(coverIdsList)
                                .authorId(authorIdsList)
                                .author_names(authorNames).build();

                        bookRepository.save(book);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }


    }


    @PostConstruct
    public void start() {
        initAuthors();
        System.out.println("Init authors done...");
        initWorks();
        System.out.println("Init works done...");
    }


    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
