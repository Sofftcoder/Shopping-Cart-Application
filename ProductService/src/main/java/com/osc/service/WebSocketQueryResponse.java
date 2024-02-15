package com.osc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.ProductServiceApplication;
import com.osc.config.HazelcastConfig;
import com.osc.config.KafkaStreamsConfig;
import com.osc.product.ProductServiceGrpc;
import com.osc.repository.ProductRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;
@Service
public class WebSocketQueryResponse extends ProductServiceGrpc.ProductServiceImplBase {
    public static Logger logger = LogManager.getLogger(ProductServiceApplication.class);
    @Autowired
    ProductRepository productRepository;
    @Autowired
    ProductManipulation productManipulation;
    @Autowired
    KafkaStreamsConfig kafkaStreamsConfig;
    Map<String, String> recentlyViewedDetails = new HashMap<>();
    @Autowired
    HazelcastConfig hazelcastConfig;
    HazelcastInstance hazelcastInstance;
    IMap<String, com.osc.entity.Products> products;
    IMap<String, List<Map<String, String>>> userRecentlyViewedData;
    IMap<String, Map<String, com.osc.entity.Products>> mappedProductAndCategoryData;

    @PostConstruct
    public void initializeHazelcastInstance() {

         hazelcastInstance = hazelcastConfig.hazelcastInstance();
         products = hazelcastInstance.getMap("ProductData");
         userRecentlyViewedData = hazelcastInstance.getMap("UserRecentlyViewedData");
         mappedProductAndCategoryData = hazelcastInstance.getMap("ProductAndCategoryData");
    }

    public String handleMT2(String prodId, String catId, String mt,String userId) {
        try {
            List<Map<String, String>> getData = userRecentlyViewedData.get(userId);

            updateRecentViewedData(getData, prodId, userId);

            Map<String, com.osc.entity.Products> allProductData = mappedProductAndCategoryData.get(catId);

            com.osc.entity.Products product = allProductData.get(prodId);

            productManipulation.updatingViewCountOfProduct(prodId);

            List<Map<String, Object>> similarProductsList = createSimilarProductsList(prodId, allProductData);

            Map<String, Object> dashBoardResponse = createDashBoardResponse(catId, prodId, mt, product, similarProductsList);

            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(dashBoardResponse);

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
        return null;
    }

    private void addNewEntry(String prodId, List<Map<String, String>> getData) {
        recentlyViewedDetails.put("prodId", prodId);
        recentlyViewedDetails.put("Counts", String.valueOf(getData.size() + 1));
        getData.add(recentlyViewedDetails);
    }

    private void updateRecentViewedData(List<Map<String, String>> getData, String prodId, String userId) throws JsonProcessingException {
        if (getData == null) {
            getData = new ArrayList<>();
        }
        if (getData.size() >= 6) {
            getData.remove(0);
        }

        if (!recentlyViewedDetails.containsValue(prodId)) {
            addNewEntry(prodId, getData);
        }
        userRecentlyViewedData.put(userId, getData);
    }

    private List<Map<String, Object>> createSimilarProductsList(String prodId, Map<String, com.osc.entity.Products> allProductData) {
        return allProductData.values().stream()
                .filter(similarProduct -> !prodId.equals(similarProduct.getProductId()))
                .limit(6)
                .map(similarProduct -> {
                    Map<String, Object> similarProductMap = new HashMap<>();
                    similarProductMap.put("productId", similarProduct.getProductId());
                    similarProductMap.put("ProdName", similarProduct.getProdName());
                    similarProductMap.put("prodMarketPrice", similarProduct.getProdMarketPrice());
                    similarProductMap.put("categoryId", similarProduct.getCategoryId());
                    return similarProductMap;
                })
                .collect(Collectors.toList());
    }

    private Map<String, Object> createDashBoardResponse(String catId, String prodId, String mt, com.osc.entity.Products product, List<Map<String, Object>> similarProductsList) {
        Map<String, Object> mergedData = new HashMap<>();
        mergedData.put("catId", catId);
        mergedData.put("prodId", prodId);
        mergedData.put("MT", mt);
        mergedData.put("prodName", product.getProdName());
        mergedData.put("prodMarketPrice", product.getProdMarketPrice());
        mergedData.put("prodDesc", product.getProductDescription());
        mergedData.put("similarProducts", similarProductsList);
        return mergedData;
    }

    public String sortPriceLowToHigh(String MT, String CatId) {
        try {
            Map<String, Map<String, com.osc.entity.Products>> allProductsMap = mappedProductAndCategoryData;
            Map<String, com.osc.entity.Products> allProductData = allProductsMap.get(CatId);
            List<com.osc.entity.Products> products = allProductData.values().stream().collect(Collectors.toList());
            List<com.osc.entity.Products> sortedProducts = products.stream()
                    .sorted(Comparator.comparing(com.osc.entity.Products::getProdMarketPrice))
                    .collect(Collectors.toList());

            List<Map<String, Object>> ProductsList = new ArrayList<>();

            for (com.osc.entity.Products product : sortedProducts) {
                Map<String, Object> productsMap = new HashMap<>();
                productsMap.put("productId", product.getProductId());
                productsMap.put("ProdName", product.getProdName());
                productsMap.put("prodMarketPrice", product.getProdMarketPrice());
                ProductsList.add(productsMap);
            }
            Map<String, Object> responseData = new HashMap<>();
            responseData.put("MT", MT);
            responseData.put("catId", CatId);
            responseData.put("products", ProductsList);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResponse = objectMapper.writeValueAsString(responseData);
            return jsonResponse;

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("An Unexpected Error Occurred" + e);
        }
        return null;
    }

    public String sortPopularity(String MT, String CatId) {
        try {
            List<com.osc.entity.Products> productByCategoryId = productRepository.findByCategoryId(CatId);
            List<Map<String, Object>> productList = new ArrayList<>();

            for (com.osc.entity.Products product : productByCategoryId) {
                Map<String, Object> productMap = new HashMap<>();
                productMap.put("productId", product.getProductId());
                productMap.put("ProdName", product.getProdName());
                productMap.put("prodMarketPrice", product.getProdMarketPrice());
                productList.add(productMap);
            }
            Map<String, Object> responseData = new HashMap<>();
            responseData.put("MT", MT);
            responseData.put("catId", CatId);
            responseData.put("products", productList);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResponse = objectMapper.writeValueAsString(responseData);
            return jsonResponse;

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
        return null;
    }

    public String sortPriceHighToLow(String MT, String CatId) {
        try {
            Map<String, Map<String, com.osc.entity.Products>> allProductsMap = mappedProductAndCategoryData;
            Map<String, com.osc.entity.Products> allProductData = allProductsMap.get(CatId);
            List<com.osc.entity.Products> productByPrice = allProductData.values().stream().collect(Collectors.toList());

            List<com.osc.entity.Products> sortedDesc = productByPrice.stream()
                    .sorted(Comparator.comparing(com.osc.entity.Products::getProdMarketPrice).reversed())
                    .collect(Collectors.toList());
            List<Map<String, Object>> productsList = new ArrayList<>();

            for (com.osc.entity.Products product : sortedDesc) {
                Map<String, Object> productMap = new HashMap<>();
                productMap.put("productId", product.getProductId());
                productMap.put("ProdName", product.getProdName());
                productMap.put("prodMarketPrice", product.getProdMarketPrice());
                productsList.add(productMap);
            }
            Map<String, Object> responseData = new HashMap<>();
            responseData.put("MT", MT);
            responseData.put("catId", CatId);
            responseData.put("products", productsList);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResponse = objectMapper.writeValueAsString(responseData);
            return jsonResponse;

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
        return null;
    }

    public String handleMT6(List<Map<String, String>> cartDetails, String MT) {
        try {
            double price = 0;
            if (cartDetails != null) {
                for (Map<String, String> cartProduct : cartDetails) {
                    String productId = cartProduct.get("prodId");

                    com.osc.entity.Products product = products.get(productId);

                    cartProduct.put("prodName", product.getProdName());
                    cartProduct.put("price", String.valueOf(product.getProdMarketPrice()));

                    double quant = Integer.parseInt(cartProduct.get("cartQty")) * product.getProdMarketPrice();
                    price += quant;
                }
            }
            Map<String, Object> responseData = new HashMap<>();
            responseData.put("MT", MT);
            responseData.put("cartProducts", cartDetails);
            responseData.put("productsCartCount", String.valueOf(cartDetails.size()));
            responseData.put("totalPrice", String.valueOf(price));

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonResponse = objectMapper.writeValueAsString(responseData);
            return jsonResponse;

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
        return null;
    }
    public void handleMT8(String value, String ProdId, String userId) {
        try {
            List<Map<String, String>> cartDetailsList = new ArrayList<>();
            if (value != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                cartDetailsList = objectMapper.readValue(value, new TypeReference<List<Map<String, String>>>() {
                });
                // Iterate through the cartList to find and update the quantity
                for (Map<String, String> item : cartDetailsList) {
                    String productId = item.get("prodId");
                    if (productId != null && productId.equals(ProdId)) {
                        // Found the item with the target Prod_Id, increase the quantity
                        int currentQuantity = Integer.parseInt(item.get("cartQty"));
                        if (currentQuantity > 0) {
                            int newQuantity = --currentQuantity;
                            item.put("cartQty", String.valueOf(newQuantity));
                        }
                    }
                }
                // Convert the updated cartList to a string
                String cart = null;
                cart = objectMapper.writeValueAsString(cartDetailsList);

                // Produce a message to the input topic
                KafkaProducer<String, String> producer = kafkaStreamsConfig.kafkaProducer();
                producer.send(new ProducerRecord<>("CARTDATA", userId, cart));
            }

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }

    public void handleMT9(String values, String ProdId, String userId) {
        try {
            int quantity = 1;
            String storeCartData = null;
            List<Map<String, String>> cartList = new ArrayList<>();
            if (values != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                cartList = objectMapper.readValue(values, new TypeReference<List<Map<String, String>>>() {
                });
            }
            // Iterate through the cartList to find and update the quantity
            for (Map<String, String> item : cartList) {
                String prodIdd = item.get("prodId");
                if (prodIdd != null && prodIdd.equals(ProdId)) {
                    // Found the item with the target Prod_Id, increase the quantity
                    int currentQuantity = Integer.parseInt(item.get("cartQty"));
                    int newQuantity = currentQuantity + 1;
                    item.put("cartQty", String.valueOf(newQuantity));
                    ObjectMapper objectMapper = new ObjectMapper();
                    storeCartData = objectMapper.writeValueAsString(cartList);

                    // Produce a message to the input topic
                    KafkaProducer<String, String> producer = kafkaStreamsConfig.kafkaProducer();
                    producer.send(new ProducerRecord<>("CARTDATA", userId, storeCartData));
                    return;
                }
            }
            Map<String, String> cartData = new HashMap<>();
            cartData.put("prodId", ProdId);
            cartData.put("cartQty", String.valueOf(quantity));
            cartList.add(cartData);
            ObjectMapper objectMapper = new ObjectMapper();
            storeCartData = objectMapper.writeValueAsString(cartList);
            KafkaProducer<String, String> producer = kafkaStreamsConfig.kafkaProducer();
            producer.send(new ProducerRecord<>("CARTDATA", userId, storeCartData));

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }
    public void handleMT10(String productData, String ProdId, String userId) {
        try {
            List<Map<String, String>> listOfCartData = new ArrayList<>();
            if (productData != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                listOfCartData = objectMapper.readValue(productData, new TypeReference<List<Map<String, String>>>() {
                });
                // Iterate through the cartList to find and update the quantity
                Iterator<Map<String, String>> iterator = listOfCartData.iterator();
                while (iterator.hasNext()) {
                    Map<String, String> item = iterator.next();
                    String prodId = item.get("prodId");
                    if (prodId != null && prodId.equals(ProdId)) {
                        iterator.remove();
                        break;
                    }
                }
                String storeProductData = null;
                // Convert the updated cartList to a string
                storeProductData = objectMapper.writeValueAsString(listOfCartData);
                // Produce a message to the input topic
                KafkaProducer<String, String> producer = kafkaStreamsConfig.kafkaProducer();
                producer.send(new ProducerRecord<>("CARTDATA", userId, storeProductData));
            }

        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }
}