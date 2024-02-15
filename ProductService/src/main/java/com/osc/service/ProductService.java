
package com.osc.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.ProductServiceApplication;
import com.osc.config.HazelcastConfig;
import com.osc.config.KafkaStreamsConfig;
import com.osc.entity.Categories;
import com.osc.entity.UserData;
import com.osc.product.*;
import com.osc.repository.CategoryRepository;
import com.osc.repository.ProductRepository;
import com.osc.repository.UserDataRepository;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import javax.annotation.PostConstruct;
import java.util.*;

import java.util.stream.Collectors;

@GrpcService
public class ProductService extends ProductServiceGrpc.ProductServiceImplBase {

    public static Logger logger = LogManager.getLogger(ProductServiceApplication.class);

    @Autowired
    ProductRepository productRepository;

    @Autowired
    UserDataRepository userDataRepository;

    @Autowired
    KafkaStreamsConfig kafkaStreamsConfig;

    @Autowired
    DashBoardDataForExistingUser dashBoardDataForExistingUser;

    @Autowired
    CategoryRepository categoryRepository;

    @Autowired
    WebSocketQuery webSocketQuery;

    @Autowired
    ProductManipulation productManipulation;
    @Autowired
    HazelcastConfig hazelcastConfig;

    HazelcastInstance hazelcastInstance;
    IMap<String, com.osc.entity.Products> products;
    IMap<String, List<Map<String, String>>> userRecentlyViewedData;
    Map<String, String> allCategories;
    IMap<String, Map<String, com.osc.entity.Products>> mappedProductAndCategoryData;

    @PostConstruct
    public void initializeHazelcastInstance() {
        hazelcastInstance = hazelcastConfig.hazelcastInstance();
        products = hazelcastInstance.getMap("ProductData");
        userRecentlyViewedData = hazelcastInstance.getMap("UserRecentlyViewedData");
        allCategories = hazelcastInstance.getMap("AllCategoriesData");

        mappedProductAndCategoryData = hazelcastInstance.getMap("ProductAndCategoryData");
    }

    /**
     * Method will get invoked automatically when Application Starts.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void applicationStartActivity() {

        /*Initialize Kafka Ktable for Storing the User Cart Details. */
        KafkaStreams streams = kafkaStreamsConfig.kafkaStreamsObjectForProductData();

        /*Initialize Kafka Ktable for Updating View Count Data in Every 30 Seconds. */
        KafkaStreams kafkaStreams = kafkaStreamsConfig.kafkaStreamObjectForCartData();

        /*loading all Product and Categories Data*/
        getAllProductsAndCategories();

        /*loading Existing User RecentlyViewed and Cart Data*/
        getExistingUserData();
    }

    /**
     * This Method will get Existing User Cart as well as Recently Viewed Product Data.
     */
    public void getExistingUserData() {
        try {
        List<UserData> userData = userDataRepository.findAll();

        for (UserData data : userData) {
            ObjectMapper objectMapper = new ObjectMapper();
                List<Map<String, String>> recentData = objectMapper.readValue(data.getRecentlyViewedDetails(), new TypeReference<List<Map<String, String>>>() {
                });

                userRecentlyViewedData.put(data.getUserId(), recentData);
                KafkaProducer<String, String> producer = kafkaStreamsConfig.kafkaProducer();
                producer.send(new ProducerRecord<>("CARTDATA", data.getUserId(), data.getCartDetails()));
        }
        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }

    /**
     * It will fetch all the Product and Category Data and add it to Hazelcast Maps.
     */
    public void getAllProductsAndCategories() {
        List<com.osc.entity.Products> productList = productRepository.findAll();
        productManipulation.getAllProduct(productList);

        for (com.osc.entity.Products product : productList) {
            products.put(product.getProductId(), product);
        }

        mappedProductAndCategoryData.putAll(productList.stream()
                .collect(Collectors.groupingBy(com.osc.entity.Products::getCategoryId,
                Collectors.toMap(com.osc.entity.Products::getProductId, products -> products))));

        List<Categories> categoriesData = categoryRepository.findAll();
        allCategories = categoriesData.stream().collect(Collectors.toMap(Categories::getCategoryId, Categories::getCategoryName));
    }

    /**
     * @param request          - It will have the Grpc Request
     * @param responseObserver returns the DashBoard data.
     */
    @Override
    public void getDashBoardData(ProductData request, StreamObserver<ProductDataResponse> responseObserver) {
        String userId = request.getRequest();
        //Recently Viewed Data
        List<Map<String, String>> userDetails = userRecentlyViewedData.get(userId);

        //need to check the condition
        if (userDetails != null) {
            //Calling the existingUser method of ProcessingProductData Class.
            responseObserver.onNext(dashBoardDataForExistingUser.existingUser(userId, userDetails, allCategories));
            responseObserver.onCompleted();

        } else {
            newUserDashBoardResponse(request, responseObserver);
        }
    }

    private void newUserDashBoardResponse(ProductData request, StreamObserver<ProductDataResponse> responseObserver) {
        //Implementation of New User DashBoard Data
        ProductDataResponse.Builder productDataResponse = ProductDataResponse.newBuilder();
        UserDashBoardData.Builder featuredResponse = buildFeaturedProductsResponse();

        // Categories
        UserDashBoardData.Builder categoriesResponse = buildCategoriesResponse();

        // List of UserData
        ListOfUserData.Builder itemsResponse = ListOfUserData.newBuilder();
        itemsResponse.addUserDashBoardData(featuredResponse);
        itemsResponse.addUserDashBoardData(categoriesResponse);

        // Build final response
        productDataResponse.setListOfUserData(itemsResponse);
        productDataResponse.setValue(true);


        ProductDataResponse response = productDataResponse.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void onRefreshProductData(ProductData request, StreamObserver<ProductDataResponse> responseObserver) {
        String userId = request.getRequest();
        List<Map<String, String>> recentlyViewedUserData = userRecentlyViewedData.get(userId);

        //need to check the condition
        if (recentlyViewedUserData != null) {
            //Calling the existingUser method of ProcessingProductData Class.
            responseObserver.onNext(dashBoardDataForExistingUser.existingUser(userId, recentlyViewedUserData,allCategories));
            responseObserver.onCompleted();

        } else {
            //Implementation of New User DashBoard Data
            newUserDashBoardResponse(request, responseObserver);
        }
    }

    /**
     * @return the Featured Product Data for New User.
     */
    private UserDashBoardData.Builder buildFeaturedProductsResponse() {
        UserDashBoardData.Builder response = UserDashBoardData.newBuilder();
        response.setTYPE("Featured Products");

        List<com.osc.entity.Products> productList = mappedProductAndCategoryData.values().stream()
                .flatMap(productMap -> productMap.values().stream())
                .sorted(Comparator.comparingInt(com.osc.entity.Products::getViewCount).reversed())
                .collect(Collectors.toList());

        for (com.osc.entity.Products value : productList) {
            com.osc.product.Products products = Products.newBuilder()
                    .setProductId(value.getProductId())
                    .setCategoryId(value.getCategoryId())
                    .setProdName(value.getProdName())
                    .setProdMarketPrice(value.getProdMarketPrice())
                    .setProductDescription(value.getProductDescription())
                    .setViewCount(value.getViewCount()).build();

            response.addProducts(products);
        }
        return response;
    }

    /**
     * @return the Categories Data for New User.
     */
    private UserDashBoardData.Builder buildCategoriesResponse() {
        UserDashBoardData.Builder response = UserDashBoardData.newBuilder();
        response.setTYPE("Categories");

        for (Map.Entry<String, String> info : allCategories.entrySet()) {
            com.osc.product.Categories categories = com.osc.product.Categories.newBuilder()
                    .setCategoryId(info.getKey())
                    .setCategoryName(info.getValue()).build();

            response.addCategories(categories);
        }

        return response;
    }

    /**
     * Grpc method to handle the Web Socket request and response.
     *
     * @param request
     * @param responseObserver
     */

    @Override
    public void socketQuery(SocketRequest request, StreamObserver<SocketResponse> responseObserver) {
        try {
            String MT = request.getMT();
            String ProdId = request.getProdId();
            String CatId = request.getCatId();
            String filter = request.getFilter();
            String userId = request.getUserId();
            webSocketQuery.mtResponse(responseObserver, MT, ProdId, CatId, filter, userId);

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }
}

