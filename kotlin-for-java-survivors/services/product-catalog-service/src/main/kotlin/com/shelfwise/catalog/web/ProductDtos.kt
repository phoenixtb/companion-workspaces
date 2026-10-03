package com.shelfwise.catalog.web

import com.shelfwise.catalog.product.Category
import com.shelfwise.catalog.product.NewProduct
import com.shelfwise.catalog.product.Product
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Positive
import jakarta.validation.constraints.Size

// Constraint annotations without a use-site target: under Kotlin 2.4's defaults they land on the
// constructor parameter and the field, and Bean Validation reads the field (Part 9).
data class CreateProductRequest(
    @Pattern(regexp = "SHW-\\d{4}", message = "must look like SHW-1234")
    val sku: String,
    @NotBlank @Size(max = 200)
    val name: String,
    val description: String = "",
    val category: Category,
    @Positive
    val pricePaise: Long,
    @NotBlank
    val supplierCode: String,
    val dietaryTags: Set<String> = emptySet(),
)

data class RepriceRequest(
    @Positive val pricePaise: Long,
)

data class ProductResponse(
    val sku: String,
    val name: String,
    val description: String,
    val category: Category,
    val pricePaise: Long,
    val price: String,
    val supplier: String,
    val dietaryTags: List<String>,
)

// Mapping as extension functions: the entity knows nothing about the web layer, and no mapper library.
fun CreateProductRequest.toNewProduct() =
    NewProduct(sku, name.trim(), description, category, pricePaise, supplierCode, dietaryTags)

fun Product.toResponse() = ProductResponse(
    sku = sku,
    name = name,
    description = description,
    category = category,
    pricePaise = pricePaise,
    price = "₹%d.%02d".format(pricePaise / 100, pricePaise % 100),
    supplier = supplier.name,
    dietaryTags = dietaryTags.sorted(),
)
