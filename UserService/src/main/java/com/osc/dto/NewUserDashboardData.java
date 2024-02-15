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
public class NewUserDashboardData {
    @JsonProperty("Featured Products")
    private List<ProductsData> featuredProducts;
    @JsonProperty("Categories")
    private List<CategoriesData> CategoriesData;
    @JsonProperty("TYPE")
    private String TYPE;
}