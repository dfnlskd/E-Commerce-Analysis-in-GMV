# Olist Ecommerce Analytics — Case Study (AWS Lakehouse)

## 📦 Project Overview

In this project, we built a GMV monitoring dashboard for the Olist e-commerce platform, using monthly data to track business performance over time. While reviewing the latest trends, we noticed that GMV dropped in August 2018, even though the number of orders continued to grow. This unusual pattern prompted a deeper investigation into what caused the decline.

To explain the GMV drop, we applied a structured decomposition framework. We first broke GMV into two components: order volume and average order value (AOV). Since order volume actually increased, we focused on AOV. We then decomposed AOV into Mix (changes in category or regional structure) and Like-for-Like (behavioral shifts within categories or regions). The results showed that Mix had little impact, while Like-for-Like was negative.

Going one level deeper, we split Like-for-Like into unit price and basket size. The analysis revealed that the AOV drop was mainly price-driven—likely due to deeper discounts—rather than a reduction in the number of items per order. We confirmed this by comparing changes in average unit price and average items per order between July and August 2018. Unit price dropped significantly, while basket size remained stable, indicating that pricing was the key driver.

This case study demonstrates how a clear, step-by-step analytical approach can uncover the real drivers behind performance shifts. Our findings suggest that the business should focus on pricing strategies within affected categories or regions, rather than trying to change product mix.
