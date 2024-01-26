
package com.osc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.entity.Categories;
import com.osc.entity.UserData;
import com.osc.product.*;
import com.osc.repository.CategoryRepository;
import com.osc.repository.ProductRepository;
import com.osc.repository.UserDataRepository;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.util.*;

import java.util.stream.Collectors;

@GrpcService
public class ProductService extends ProductServiceGrpc.ProductServiceImplBase {

    @Autowired
    ProductRepository productRepository;

    @Autowired
    UserDataRepository userDataRepository;

    @Autowired
    ProcessingProductData processingProductData;

    @Autowired
    CategoryRepository categoryRepository;

    @Autowired
    WebSocketQuery webSocketQuery;

    @Autowired
    ProductManipulation productManipulation;

    HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();

    ISet<String> distributedSet = hazelcastInstance.getSet("User Data");

    IMap<String, List<Map<String, String>>> limitedMap = hazelcastInstance.getMap("limitedMap");
    Map<String, String> allCategories = hazelcastInstance.getMap("AllCategoriesData");
    Map<String, Map<String, com.osc.entity.Products>> allProductsMap = new HashMap<>();

    IMap<String, Map<String, Map<String, com.osc.entity.Products>>> allProductsImap = hazelcastInstance.getMap("AllProductsMap");

    /**
     * Method will get invoked automatically when Application Starts.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void getProductData() {
        productManipulation.updateViewCount();
        productManipulation.updateCartInfo();
        getAllProductsAndCategories();
        getExistingUserData();
    }

    /**
     * This Method will get Existing User Cart as well as Recently Viewed Product Data.
     */
    public void getExistingUserData() {
        List<UserData> userData = userDataRepository.findAll();
        for (UserData data : userData) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                List<Map<String, String>> recentData = objectMapper.readValue(data.getRecentlyViewedDetails(), new TypeReference<List<Map<String, String>>>() {
                });

                limitedMap.put(data.getUserId(), recentData);
                productManipulation.kafkaMessageProducer("CARTDATA", data.getUserId(), data.getCartDetails());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * It will fetch all the Product and Category Data and add it to Hazelcast Maps.
     */
    public void getAllProductsAndCategories() {
        List<com.osc.entity.Products> productsMap = productRepository.findAll();
        productManipulation.getAllProduct(productsMap);
        allProductsMap = productsMap.stream().collect(Collectors.groupingBy(com.osc.entity.Products::getCategoryId,
                Collectors.toMap(com.osc.entity.Products::getProductId, p -> p)));
        allProductsImap.put("ProductData", allProductsMap);
        List<Categories> categoriesData = categoryRepository.findAll();
        allCategories = categoriesData.stream().collect(Collectors.toMap(Categories::getCategoryId, Categories::getCategoryName));
    }

    /**
     * @param request          - It will have the Grpc Request
     * @param responseObserver returns the DashBoard data.
     */
    @Override
    public void getProductData(ProductData request, StreamObserver<ProductDataResponse> responseObserver) {
        String userId = distributedSet.iterator().next();
        UserData userDetails = userDataRepository.findRecentlyViewedDataByUserId(userId);

        //need to check the condition
        if (userDetails != null) {

            //Calling the existingUser method of ProcessingProductData Class.
            responseObserver.onNext(processingProductData.existingUser(userId, userDetails, allProductsMap, allCategories));
            responseObserver.onCompleted();

        } else {
            //Implementation of New User DashBoard Data
            String data = request.getRequest();
            UserData newUser = new UserData();
            newUser.setUserId(userId);
            ProductDataResponse.Builder productDataResponse = ProductDataResponse.newBuilder();
            if (data.equals("All")) {
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
            }

            ProductDataResponse response = productDataResponse.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * @return the Featured Product Data for New User.
     */
    private UserDashBoardData.Builder buildFeaturedProductsResponse() {
        UserDashBoardData.Builder response = UserDashBoardData.newBuilder();
        response.setTYPE("Featured Products");
        List<com.osc.entity.Products> productList = allProductsMap.values().stream()
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
     * @param request          - It will have MT request.
     * @param responseObserver - returns the Data according to MT request.
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
            e.printStackTrace();
        }
    }
}

