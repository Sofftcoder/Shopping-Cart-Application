package com.osc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductsData {
    private String productId;

    private String categoryId;

    private String ProdName;

    private Float prodMarketPrice;

    private String ProductDescription;

    private Integer ViewCount;
}
