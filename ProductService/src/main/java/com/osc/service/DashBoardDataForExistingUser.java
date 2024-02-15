package com.osc.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.ProductServiceApplication;
import com.osc.config.HazelcastConfig;
import com.osc.product.ListOfUserData;
import com.osc.product.ProductDataResponse;
import com.osc.product.Products;
import com.osc.product.UserDashBoardData;
import com.osc.repository.ProductRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DashBoardDataForExistingUser {
    public static Logger logger = LogManager.getLogger(ProductServiceApplication.class);
    @Autowired
    ProductRepository productRepository;
    @Autowired
    WebSocketQuery webSocketQuery;
    @Autowired
    HazelcastConfig hazelcastConfig;
    HazelcastInstance hazelcastInstance;
    IMap<String, Map<String, com.osc.entity.Products>> mappedProductAndCategoryData;
    @PostConstruct
    public void initializeHazelcastInstance() {
        hazelcastInstance = hazelcastConfig.hazelcastInstance();
        mappedProductAndCategoryData = hazelcastInstance.getMap("ProductAndCategoryData");
    }
    /**
     * @param userId
     * @param recentlyViewedUserData     - Consist of Cart and Recently Viewed Data.
     * @return
     */
    public ProductDataResponse existingUser(String userId, List<Map<String, String>> recentlyViewedUserData,Map<String, String> allCategories) {
        try {
            //Parse recently viewed data

            //Build Recently Viewed Products response
            UserDashBoardData.Builder recentlyViewedBuilder = buildRecentlyViewedResponse(recentlyViewedUserData, mappedProductAndCategoryData);

            //Build Categories response
            UserDashBoardData.Builder categoryBuilder = buildCategoriesResponse(allCategories);

            //Generate Similar Products response
            UserDashBoardData.Builder similarProductsBuilder = buildSimilarProductsResponse(recentlyViewedBuilder.getProductsList());

            //Generate Cart response
            UserDashBoardData.Builder cartBuilder = buildCartResponse(userId, mappedProductAndCategoryData);

            //Combine all responses into a single response
            ListOfUserData.Builder itemsResponse = combineDashboardResponses(recentlyViewedBuilder, categoryBuilder, cartBuilder, similarProductsBuilder);

            //Build final ProductDataResponse
            ProductDataResponse.Builder productDataResponse = buildDashboardDataResponse(itemsResponse);

            //Set additional values for the response
            productDataResponse.setValue(false);

            //Return the final response
            return productDataResponse.build();

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }

        return null;
    }

    // Helper method to build Recently Viewed Products response

    /**
     * @param recentlyViewedData - Contains Product Id
     * @param allProductsData    - Contains All Product Information.
     * @return
     */
    private UserDashBoardData.Builder buildRecentlyViewedResponse(List<Map<String, String>> recentlyViewedData, Map<String, Map<String, com.osc.entity.Products>> allProductsData) {
        UserDashBoardData.Builder recentlyViewedBuilder = UserDashBoardData.newBuilder();
        recentlyViewedBuilder.setTYPE("Recently Viewed Products");

        /* It will traverse the Recently Viewed Data and get the Product Id from it
        and fetch the corresponding information from allProductData. */
        for (Map<String, String> val : recentlyViewedData) {
            String productId = val.get("prodId");
            for (Map.Entry<String, Map<String, com.osc.entity.Products>> allProductsMap : allProductsData.entrySet()) {
                com.osc.entity.Products productData = allProductsMap.getValue().get(productId);

                com.osc.product.Products.Builder productBuilder = com.osc.product.Products.newBuilder();
                if (productData != null) {
                    productBuilder.setProductId(val.get("prodId"));
                    productBuilder.setCategoryId(productData.getCategoryId());
                    productBuilder.setProdName(productData.getProdName());
                    productBuilder.setProdMarketPrice(productData.getProdMarketPrice());
                    productBuilder.setProductDescription(productData.getProductDescription());
                    productBuilder.setViewCount(Integer.parseInt(val.get("Counts")));

                    recentlyViewedBuilder.addProducts(productBuilder.build());
                }
            }
        }
        return recentlyViewedBuilder;
    }

    // Helper method to build Categories response
    private UserDashBoardData.Builder buildCategoriesResponse(Map<String, String> allCategories) {
        UserDashBoardData.Builder categoryBuilder = UserDashBoardData.newBuilder();
        categoryBuilder.setTYPE("Categories");

        for (Map.Entry<String, String> info : allCategories.entrySet()) {
            com.osc.product.Categories categories = com.osc.product.Categories.newBuilder()
                    .setCategoryId(info.getKey())
                    .setCategoryName(info.getValue()).build();
            categoryBuilder.addCategories(categories);
        }

        return categoryBuilder;
    }

    // Helper method to build Similar Products response on the Recently Viewed Products.
    private UserDashBoardData.Builder buildSimilarProductsResponse(List<Products> recentlyViewedProductsList) {
        try {
            UserDashBoardData.Builder similarProductsBuilder = UserDashBoardData.newBuilder();
            List<com.osc.entity.Products> similarProductList = new ArrayList<>();

            similarProductsBuilder.setTYPE("Similar Products");

            List<String> productIds = recentlyViewedProductsList.stream()
                    .map(Products::getProductId)
                    .collect(Collectors.toList());

            for (Products products : recentlyViewedProductsList) {
                int limit = 1;
                int offset = 0;
                do {
                    com.osc.entity.Products dataUser = productRepository.findTopByCategoryId(products.getCategoryId(), limit, offset);
                    // Checking if the product ID of recentlyViewedData is equal to the product ID coming from DB
                    boolean productIdNotEqual = productIds.stream()
                            .anyMatch(productId -> productId.equals(dataUser.getProductId()));

                    // Checking if the product is already present in Similar Product List.
                    boolean anyProductAlreadyPresent = similarProductList.stream()
                            .anyMatch(product -> product.getProductId().equals(dataUser.getProductId()));

                    if (anyProductAlreadyPresent || productIdNotEqual) {
                        // Product found in either updatedList or productIds
                        offset++;
                    } else {
                        // Product is unique, add it to updatedList
                        similarProductList.add(dataUser);
                        break;
                    }
                } while (offset < 12);
            }
            //need to check if the similar product having less than 6 products.
            lessThanSixSimilarProductResponse(similarProductList, productIds);

            ObjectMapper objectMapper = new ObjectMapper();
            String upDatedProductList = objectMapper.writeValueAsString(similarProductList);
            similarProductsBuilder.setSimilarProducts(upDatedProductList);
            return similarProductsBuilder;

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("An Unexpected Error Occurred" + e);
        }
        return null;
    }

    /**
     *
     * @param similarProductList
     * @param productIds - List of Products Ids Present in Recently Viewed Products.
     */
    private void lessThanSixSimilarProductResponse(List<com.osc.entity.Products> similarProductList, List<String> productIds) {
        while (similarProductList.size() < 6) {
            com.osc.entity.Products productDetails = similarProductList.get(similarProductList.size() - 1);
            int limit = 1;
            int offset = 0;
            do {
                com.osc.entity.Products dataUser = productRepository.findTopByCategoryId(productDetails.getCategoryId(), limit, offset);

                // Checking if the product ID of recentlyViewedData is equal to the product ID coming from DB
                boolean productIdNotEqual = productIds.stream()
                        .anyMatch(productId -> productId.equals(dataUser.getProductId()));

                // Checking if the product is already present in similarProductList
                boolean anyProductAlreadyPresent = similarProductList.stream()
                        .anyMatch(product -> product.getProductId().equals(dataUser.getProductId()));

                if (anyProductAlreadyPresent || productIdNotEqual) {
                    // Product found in either updatedList or productIds
                    offset++;
                } else {
                    // Product is unique, add it to updatedList
                    similarProductList.add(dataUser);
                    break;
                }
            } while (offset < 11);
        }
    }



    /**
     * Helper method to build Cart response
     * @param userId
     * @param allProductsData - Contains all Products Info in form of Map<CatId,Map<ProdId,Product>>
     * @return
     */
    private UserDashBoardData.Builder buildCartResponse(String userId, Map<String, Map<String, com.osc.entity.Products>> allProductsData) {
        try {
            List<Map<String, String>> list = new ArrayList<>();

            UserDashBoardData.Builder cartBuilder = UserDashBoardData.newBuilder();
            String listOfValues = webSocketQuery.getUserCartDataFromKTable(userId);

            if (listOfValues != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                list = objectMapper.readValue(listOfValues, new TypeReference<>() {
                });
            }

            double price = 0;

            cartBuilder.setTYPE("Cart");

            for (Map<String, String> productData : list) {
                String productId = productData.get("prodId");
                for(Map.Entry<String,Map<String, com.osc.entity.Products>> allProductData: allProductsData.entrySet()){
                    com.osc.entity.Products products = allProductData.getValue().get(productId);
                    if (products != null) {
                        productData.put("prodName", products.getProdName());
                        productData.put("price", String.valueOf(products.getProdMarketPrice()));

                        double quantity = Integer.parseInt(productData.get("cartQty")) * products.getProdMarketPrice();
                        price += quantity;
                    }
                }
            }

            Map<String, Object> responseData = new HashMap<>();
            responseData.put("cartProducts", list);
            responseData.put("ProductsCartCount", String.valueOf(list.size()));
            responseData.put("totalPrice", String.valueOf(price));
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResponse = objectMapper.writeValueAsString(responseData);

            cartBuilder.setCart(jsonResponse);
            return cartBuilder;

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
        return null;
    }
    /**
     * Helper method to combine all responses into a single response
     * @param recentlyViewedBuilder - Contains RecentlyViewedProducts
     * @param categoryBuilder - Contains Categories Information.
     * @param cartBuilder  - Contains Cart Data.
     * @param similarProductsBuilder - Contains Similar Product Data
     * @return
     */
    private ListOfUserData.Builder combineDashboardResponses(
            UserDashBoardData.Builder recentlyViewedBuilder,
            UserDashBoardData.Builder categoryBuilder,
            UserDashBoardData.Builder cartBuilder,
            UserDashBoardData.Builder similarProductsBuilder) {
        ListOfUserData.Builder itemsResponse = ListOfUserData.newBuilder();
        itemsResponse.addUserDashBoardData(recentlyViewedBuilder);
        itemsResponse.addUserDashBoardData(categoryBuilder);
        itemsResponse.addUserDashBoardData(cartBuilder);
        itemsResponse.addUserDashBoardData(similarProductsBuilder);

        return itemsResponse;
    }

    /**
     * Helper method to build final ProductDataResponse
     * @param itemsResponse - Contains all the Data i.e RecentlyViewed, Categories, Similar Product and Cart.
     * @return
     */
    private ProductDataResponse.Builder buildDashboardDataResponse(ListOfUserData.Builder itemsResponse) {
        ProductDataResponse.Builder productDataResponse = ProductDataResponse.newBuilder();
        productDataResponse.setListOfUserData(itemsResponse);
        return productDataResponse;
    }

}