package com.osc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.osc.dto.*;
import com.osc.product.ListOfUserData;
import com.osc.product.UserDashBoardData;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserDashBoardDetails {
    public void newUserDashboard(ListOfUserData grpcResponse, List<NewUserDashboardData> userDashboardData) {
        for (UserDashBoardData userData : grpcResponse.getUserDashBoardDataList()) {
            //Adding Featured Product Data.
            NewUserDashboardData newUserDashboardData = new NewUserDashboardData();
            newUserDashboardData.setTYPE(userData.getTYPE());

            List<ProductsData> dtoProductList = getProductsData(userData);
            newUserDashboardData.setFeaturedProducts(dtoProductList);

            //Adding Categories Data.
            List<CategoriesData> dtoCategoryList = new ArrayList<>();
            for (com.osc.product.Categories grpcCategory : userData.getCategoriesList()) {
                CategoriesData dtoCategory = new CategoriesData();
                dtoCategory.setCategoryId(grpcCategory.getCategoryId());
                dtoCategory.setCategoryName(grpcCategory.getCategoryName());
                dtoCategoryList.add(dtoCategory);
            }
            newUserDashboardData.setCategoriesData(dtoCategoryList);
            userDashboardData.add(newUserDashboardData);
        }
    }

    private List<ProductsData> getProductsData(UserDashBoardData userData) {
        List<ProductsData> dtoProductList = new ArrayList<>();
        for (com.osc.product.Products grpcProduct : userData.getProductsList()) {
            ProductsData dtoProduct = new ProductsData();
            dtoProduct.setProductId(grpcProduct.getProductId());
            dtoProduct.setCategoryId(grpcProduct.getCategoryId());
            dtoProduct.setProdName(grpcProduct.getProdName());
            dtoProduct.setProdMarketPrice(grpcProduct.getProdMarketPrice());
            dtoProduct.setProductDescription(grpcProduct.getProductDescription());
            dtoProduct.setViewCount(grpcProduct.getViewCount());
            dtoProductList.add(dtoProduct);
        }
        return dtoProductList;
    }

    public void existingUserDashboard(String userId, ListOfUserData grpcResponse, List<ExistingUserDashboardData> userDashboardData) {
        for (UserDashBoardData userData : grpcResponse.getUserDashBoardDataList()) {

            //Adding Recently Viewed Data.
            ExistingUserDashboardData existingUserDashboardData = new ExistingUserDashboardData();
            existingUserDashboardData.setTYPE(userData.getTYPE());

            List<ProductsData> dtoProductList = getProductsData(userData);
            existingUserDashboardData.setRecentlyViewedProducts(dtoProductList);

            List<com.osc.dto.Categories> dtoCategoryList = new ArrayList<>();
            for (com.osc.product.Categories grpcCategory : userData.getCategoriesList()) {
                com.osc.dto.Categories dtoCategory = new com.osc.dto.Categories();
                dtoCategory.setCategoryId(grpcCategory.getCategoryId());
                dtoCategory.setCategoryName(grpcCategory.getCategoryName());
                dtoCategoryList.add(dtoCategory);
            }
            existingUserDashboardData.setCategories(dtoCategoryList);


            //Adding Similar Products Data.
            Gson gson = new Gson();
            java.lang.reflect.Type listType = new TypeToken<List<ProductsData>>() {
            }.getType();
            List<ProductsData> productList = gson.fromJson(userData.getSimilarProducts(), listType);
            existingUserDashboardData.setSimilarProducts(productList);

            String cartJson = userData.getCart();
            Cart cart = new Cart();
            if (cartJson != null) {
                Cart cartResponse = gson.fromJson(cartJson, Cart.class);

                // Checking if cartResponse is not null before accessing its methods
                if (cartResponse != null) {
                    cart.setUserId(userId);
                    cart.setProductsCartCount(cartResponse.getProductsCartCount());
                    cart.setCartProducts(cartResponse.getCartProducts());
                    cart.setTotalPrice(cartResponse.getTotalPrice());
                }
            }
            existingUserDashboardData.setCART(cart);
            userDashboardData.add(existingUserDashboardData);
        }
    }
}
