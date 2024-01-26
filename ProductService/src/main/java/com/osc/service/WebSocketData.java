
package com.osc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.entity.UserData;
import com.osc.product.ProductServiceGrpc;
import com.osc.repository.ProductRepository;
import com.osc.repository.UserDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This Class having different methods to handle different MT request and Return the response.
 */
@Component
public class WebSocketData extends ProductServiceGrpc.ProductServiceImplBase {

    @Autowired
    ProductRepository productRepository;

    @Autowired
    ProductManipulation productManipulation;

    @Autowired
    UserDataRepository userDataRepository;

    Map<String, String> recentlyViewedDetails = new HashMap<>();

    ObjectMapper objectMapper = new ObjectMapper();

    HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();

    ISet<String> distributedSet = hazelcastInstance.getSet("User Data");
    IMap<String, String> recentlyViewedData = hazelcastInstance.getMap("Recently Viewed Data");

    IMap<String, List<Map<String, String>>> limitedMap = hazelcastInstance.getMap("limitedMap");
    int maxSize = 6;
    IMap<String, Map<String, Map<String, com.osc.entity.Products>>> allProductsImap = hazelcastInstance.getMap("AllProductsMap");

    public String handleMT2(String prodId, String catId, String userId, String mt) {
        try {
            String loggedInUser = distributedSet.iterator().next();
            UserData userData = userDataRepository.findRecentlyViewedDataByUserId(userId);
            if (userData != null) {
                List<Map<String, String>> recentData = objectMapper.readValue(userData.getRecentlyViewedDetails(), new TypeReference<List<Map<String, String>>>() {
                });
                limitedMap.put(userId, recentData);
            }
            List<Map<String, String>> getData = limitedMap.get(loggedInUser);
            if (getData == null) {
                getData = new ArrayList<>();
            }
            removeOldestEntryIfNeeded(getData);

            if (isNewProdId(prodId)) {
                addNewEntry(prodId, getData);
            }
            limitedMap.put(loggedInUser, getData);

            updateRecentViewedData();

            Map<String, Map<String, com.osc.entity.Products>> allProductsMap = allProductsImap.get("ProductData");

            Map<String, com.osc.entity.Products> allProductData = allProductsMap.get(catId);
            com.osc.entity.Products product = allProductData.get(prodId);

            productManipulation.updatingViewCountOfProduct(prodId);

            List<Map<String, Object>> similarProductsList = createSimilarProductsList(prodId, allProductData);

            Map<String, Object> mergedData = createMergedData(catId, prodId, mt, product, similarProductsList);

            return objectMapper.writeValueAsString(mergedData);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void removeOldestEntryIfNeeded(List<Map<String, String>> getData) {
        if (getData.size() >= maxSize) {
            getData.remove(0);
        }
    }

    private boolean isNewProdId(String prodId) {
        return !recentlyViewedDetails.containsValue(prodId);
    }

    private void addNewEntry(String prodId, List<Map<String, String>> getData) {
        recentlyViewedDetails.put("prodId", prodId);
        recentlyViewedDetails.put("Counts", String.valueOf(getData.size() + 1));
        getData.add(new HashMap<>(recentlyViewedDetails));
    }

    private void updateRecentViewedData() throws JsonProcessingException {
        String vaal = objectMapper.writeValueAsString(limitedMap);
        recentlyViewedData.put("Recently Viewed Products", vaal);
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

    private Map<String, Object> createMergedData(String catId, String prodId, String mt, com.osc.entity.Products product, List<Map<String, Object>> similarProductsList) {
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

    public String handleFilterLH(String MT, String CatId) {
        try {
            Map<String, Map<String, com.osc.entity.Products>> allProductsMap = allProductsImap.get("ProductData");
            Map<String, com.osc.entity.Products> allProductData = allProductsMap.get(CatId);
            List<com.osc.entity.Products> products = allProductData.values().stream().collect(Collectors.toList());
            List<com.osc.entity.Products> sortedAsc = products.stream()
                    .sorted(Comparator.comparing(com.osc.entity.Products::getProdMarketPrice))
                    .collect(Collectors.toList());

            List<Map<String, Object>> similarProductsLisst = new ArrayList<>();

            for (com.osc.entity.Products similarProduct : sortedAsc) {
                Map<String, Object> similarProductMap = new HashMap<>();
                similarProductMap.put("productId", similarProduct.getProductId());
                similarProductMap.put("ProdName", similarProduct.getProdName());
                similarProductMap.put("prodMarketPrice", similarProduct.getProdMarketPrice());
                similarProductsLisst.add(similarProductMap);
            }
            Map<String, Object> allCombinedData = new HashMap<>();
            allCombinedData.put("MT", MT);
            allCombinedData.put("catId", CatId);
            allCombinedData.put("products", similarProductsLisst);

            String json_Response = objectMapper.writeValueAsString(allCombinedData);
            return json_Response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String handleFilterP(String MT, String CatId) {
        try {
            List<com.osc.entity.Products> productByCategoryId = productRepository.findByCategoryId(CatId);
            List<Map<String, Object>> similarProductsListt = new ArrayList<>();

            for (com.osc.entity.Products similarProduct : productByCategoryId) {
                Map<String, Object> similarProductMap = new HashMap<>();
                similarProductMap.put("productId", similarProduct.getProductId());
                similarProductMap.put("ProdName", similarProduct.getProdName());
                similarProductMap.put("prodMarketPrice", similarProduct.getProdMarketPrice());
                similarProductsListt.add(similarProductMap);
            }
            Map<String, Object> merged_Data = new HashMap<>();
            merged_Data.put("MT", MT);
            merged_Data.put("catId", CatId);
            merged_Data.put("products", similarProductsListt);

            String json = objectMapper.writeValueAsString(merged_Data);
            return json;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String handleFilterHL(String MT, String CatId) {
        try {
            Map<String, Map<String, com.osc.entity.Products>> allProductsMap = allProductsImap.get("ProductData");
            Map<String, com.osc.entity.Products> allProductData = allProductsMap.get(CatId);
            List<com.osc.entity.Products> productByPrice = allProductData.values().stream().collect(Collectors.toList());
            List<com.osc.entity.Products> sortedDesc = productByPrice.stream()
                    .sorted(Comparator.comparing(com.osc.entity.Products::getProdMarketPrice).reversed())
                    .collect(Collectors.toList());
            List<Map<String, Object>> productsList = new ArrayList<>();

            for (com.osc.entity.Products similarProduct : sortedDesc) {
                Map<String, Object> similarProductMap = new HashMap<>();
                similarProductMap.put("productId", similarProduct.getProductId());
                similarProductMap.put("ProdName", similarProduct.getProdName());
                similarProductMap.put("prodMarketPrice", similarProduct.getProdMarketPrice());
                productsList.add(similarProductMap);
            }
            Map<String, Object> newData = new HashMap<>();
            newData.put("MT", MT);
            newData.put("catId", CatId);
            newData.put("products", productsList);

            String responseJson = objectMapper.writeValueAsString(newData);
            return responseJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    public String handleFilterNF(String MT, String CatId) {
        try {
            List<com.osc.entity.Products> productByCategoryId = productRepository.findByCategoryId(CatId);
            List<Map<String, Object>> similarProductsListt = new ArrayList<>();
            productByCategoryId = productRepository.findByCategoryId(CatId);
            similarProductsListt = new ArrayList<>();

            for (com.osc.entity.Products similarProduct : productByCategoryId) {
                Map<String, Object> similarProductMap = new HashMap<>();
                similarProductMap.put("productId", similarProduct.getProductId());
                similarProductMap.put("ProdName", similarProduct.getProdName());
                similarProductMap.put("prodMarketPrice", similarProduct.getProdMarketPrice());
                similarProductsListt.add(similarProductMap);
            }
            Map<String, Object> new_Data = new HashMap<>();
            new_Data.put("MT", MT);
            new_Data.put("catId", CatId);
            new_Data.put("products", similarProductsListt);
            String newJson = objectMapper.writeValueAsString(new_Data);
            return newJson;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String handleMT6(List<Map<String, String>> cartDetails, String MT) {
        try {
            double price = 0;
            if (cartDetails != null) {
                for (Map<String, String> val : cartDetails) {
                    String productId = val.get("prodId");
                    com.osc.entity.Products prp = productRepository.findByProductId(productId);

                    val.put("prodName", prp.getProdName());
                    val.put("price", String.valueOf(prp.getProdMarketPrice()));

                    double quant = Integer.valueOf(val.get("cartQty")) * prp.getProdMarketPrice();
                    price += quant;
                }
            }

            Map<String, Object> responseData = new HashMap<>();
            responseData.put("MT", MT);
            responseData.put("cartProducts", cartDetails);
            responseData.put("productsCartCount", String.valueOf(cartDetails.size()));
            responseData.put("totalPrice", String.valueOf(price));
            String json = objectMapper.writeValueAsString(responseData);
            return json;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void handleMT8(String MT, String value, String ProdId, String userId) {
        try {
            List<Map<String, String>> cartDetailsList = new ArrayList<>();
            if (value != null && value.matches("\\p{Print}+")) {
                try {
                    cartDetailsList = objectMapper.readValue(value, new TypeReference<List<Map<String, String>>>() {
                    });
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                // Iterate through the cartList to find and update the quantity
                for (Map<String, String> item : cartDetailsList) {
                    String prodIdd = item.get("prodId");
                    if (prodIdd != null && prodIdd.equals(ProdId)) {
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
                try {
                    cart = objectMapper.writeValueAsString(cartDetailsList);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                // Produce a message to the input topic
                productManipulation.kafkaMessageProducer("CARTDATA", userId, cart);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleMT9(String values, String ProdId, String userId) {
        try {
            int quantity = 1;
            String storeData = null;
            List<Map<String, String>> cartList = new ArrayList<>();
            if (values != null) {
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
                    try {
                        storeData = objectMapper.writeValueAsString(cartList);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    // Produce a message to the input topic
                    productManipulation.kafkaMessageProducer("CARTDATA", userId, storeData);
                    return;
                }
            }
            Map<String, String> datas = new HashMap<>();
            datas.put("prodId", ProdId);
            datas.put("cartQty", String.valueOf(quantity));
            cartList.add(datas);
            storeData = objectMapper.writeValueAsString(cartList);
            productManipulation.kafkaMessageProducer("CARTDATA", userId, storeData);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleMT10(String productData, String ProdId, String userId) {
        try {
            List<Map<String, String>> listOfCart = new ArrayList<>();
            if (productData != null && productData.matches("\\p{Print}+")) {
                try {
                    listOfCart = objectMapper.readValue(productData, new TypeReference<List<Map<String, String>>>() {
                    });
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                // Iterate through the cartList to find and update the quantity
                Iterator<Map<String, String>> iterator = listOfCart.iterator();
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
                try {
                    storeProductData = objectMapper.writeValueAsString(listOfCart);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                // Produce a message to the input topic
                productManipulation.kafkaMessageProducer("CARTDATA", userId, storeProductData);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}