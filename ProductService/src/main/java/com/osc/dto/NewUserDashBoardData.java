package com.osc.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class NewUserDashBoardData {
    @JsonProperty("Featured Products")
    private List<ProductsData> featuredProducts;
    @JsonProperty("Categories")
    private List<CategoriesData> CategoriesData;
    @JsonProperty("TYPE")
    private String TYPE;
}