package com.osc.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExistingUserDashboardData {
    @JsonProperty("Recently Viewed Products")
    private List<ProductsData> recentlyViewedProducts;
    @JsonProperty("Categories")
    private List<Categories> Categories;
    @JsonProperty("TYPE")
    private String TYPE;
    @JsonProperty("Cart")
    private Cart CART;
    @JsonProperty("Similar Products")
    private List<ProductsData> similarProducts;
}