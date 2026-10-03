package com.shelfwise.catalog.web

import com.shelfwise.catalog.product.CatalogException
import com.shelfwise.catalog.product.DuplicateSku
import com.shelfwise.catalog.product.ProductNotFound
import com.shelfwise.catalog.product.UnknownSupplier
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.orm.ObjectOptimisticLockingFailureException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.context.request.WebRequest
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler

// RFC 9457 problem details for every error. The base class already covers Spring MVC's own exceptions
// (unreadable JSON, missing parameters, wrong types); this adds the catalog's and the field errors.
@RestControllerAdvice
class ProblemHandling : ResponseEntityExceptionHandler() {
    @ExceptionHandler
    fun catalogFailure(e: CatalogException): ProblemDetail = when (e) { // exhaustive: no else branch
        is ProductNotFound -> problem(HttpStatus.NOT_FOUND, "Product not found", e).apply { setProperty("sku", e.sku) }
        is DuplicateSku -> problem(HttpStatus.CONFLICT, "Duplicate SKU", e).apply { setProperty("sku", e.sku) }
        is UnknownSupplier -> problem(HttpStatus.BAD_REQUEST, "Unknown supplier", e).apply { setProperty("supplierCode", e.code) }
    }

    // Two requests changed the same product; @Version detected it, and the later one loses.
    @ExceptionHandler
    fun concurrentUpdate(e: ObjectOptimisticLockingFailureException): ProblemDetail =
        problem(HttpStatus.CONFLICT, "Concurrent update", e).apply { detail = "The product changed while this request ran; retry it." }

    // A database constraint caught what the code didn't, such as two concurrent creates of one SKU.
    @ExceptionHandler
    fun constraintViolation(e: DataIntegrityViolationException): ProblemDetail =
        ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "The change conflicts with existing data.").apply {
            title = "Constraint violated" // no SQL in the response
        }

    override fun handleMethodArgumentNotValid(
        ex: MethodArgumentNotValidException,
        headers: HttpHeaders,
        status: HttpStatusCode,
        request: WebRequest,
    ): ResponseEntity<Any>? {
        val body = ex.body.apply {
            setProperty("errors", ex.bindingResult.fieldErrors.associate { it.field to it.defaultMessage })
        }
        return handleExceptionInternal(ex, body, headers, status, request)
    }

    private fun problem(status: HttpStatus, title: String, e: Exception) =
        ProblemDetail.forStatusAndDetail(status, e.message).apply { this.title = title }
}
