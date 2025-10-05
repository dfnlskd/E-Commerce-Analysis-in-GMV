# Olist Ecommerce Analytics in GMV — Case Study (AWS Lakehouse)

## 📦 Project Overview

In this project, we built a GMV monitoring dashboard for the Olist e-commerce platform, using monthly data to track business performance over time. While reviewing the latest trends, we noticed that GMV dropped in August 2018, even though the number of orders continued to grow. This unusual pattern prompted a deeper investigation into what caused the decline.

To explain the GMV drop, we applied a structured decomposition framework. We first broke GMV into two components: order volume and average order value (AOV). Since order volume actually increased, we focused on AOV. We then decomposed AOV into Mix (changes in category or regional structure) and Like-for-Like (behavioral shifts within categories or regions). The results showed that Mix had little impact, while Like-for-Like was negative.

This case study demonstrates how a clear, step-by-step analytical approach can uncover the real drivers behind performance shifts. Our findings suggest that the business should focus on pricing strategies within affected categories or regions, rather than trying to change product mix.

## 🔍 2. Analytical Framework

To understand the drivers behind GMV changes, we designed a layered analysis framework that moves from high-level trends to granular insights:

- **L0**: Track monthly GMV, orders, and AOV to spot anomalies.
- **L1**: Decompose GMV into order volume × AOV.
- **L2-A**: Split AOV into Mix (structure) and Like-for-Like (within-segment behavior).
- **L2-B**: Decompose Like-for-Like into Price (unit price) and Basket (items per order).
- **L3**: Drill down into specific categories and states.

This structured breakdown helps isolate root causes and avoid misleading conclusions based on surface-level metrics.

## 🛠️ 3. Architecture & Stack

We implemented a Lakehouse-style analytics pipeline on AWS:

- **Storage Layer**: Data is stored in Amazon S3 using a three-tier structure:
  - *Bronze*: Raw input data
  - *Silver*: Cleaned and transformed tables
  - *Gold*: Aggregated tables for analysis

- **Processing Layer**: We used Amazon Athena for querying data directly on S3 using SQL. This enabled fast iteration and cost-effective analytics.
  
- **Serving Layer**: For visualization, we used Tableau to build dashboards that tracked GMV, order trends, and AOV decomposition.
  
This architecture enables fast iteration, clear data lineage, and a scalable foundation for deeper analytics.

## 📊 4. Step-by-Step Analysis

### L0: Monthly GMV Monitoring

We regularly monitor monthly GMV to track overall business performance on the Olist platform. From the "Monthly GMV" chart, we observed that while GMV had shown steady growth through 2017 and 2018, the latest data point—**August 2018**—showed a decline compared to previous months.

To better understand this change, we looked at key metrics for August 2018:
- **GMV**: 838,577
- **Orders**: 6,351
- **AOV (Average Order Value)**: 132

This observation led us to examine whether the GMV change was driven by order volume or by order value.

<img width="901" height="720" alt="Picture1" src="https://github.com/user-attachments/assets/f6fec512-8d19-4827-818c-552a89c585fd" />
Dashboard 1: Monthly GMV Tracking Overall Business Performance

### L1: Decomposing GMV — Orders × AOV

The “Monthly Orders & AOV” chart reveals that in August 2018:
- The number of orders increased slightly compared to July.
- The AOV dropped sharply, reaching one of the lowest values of the year.

Since GMV = Orders × AOV, and order volume actually grew, the GMV decline must have come from a drop in AOV.

To validate this, the "GMV Waterfall of August 2018" further confirms that AOV contributed negatively, while the volume (orders) contributed positively.

**This establishes that the GMV drop in August 2018 was not due to fewer orders, but rather due to lower value per order—prompting further analysis at the AOV level.**

### L2-A: Decomposing AOV — Mix vs. Like-for-Like (LFL)

To furthur understand why AOV dropped in August 2018, we decomposed it into two components:

- **Mix**: Changes in the structure of transactions across categories or states.
- **Like-for-Like (LFL)**: Changes in customer behavior within the same category or region.

From the two waterfall charts—"AOV LFL Waterfall by State" and "by Category"—we see that the Mix effect was minimal, with almost no structural shift in state or category composition. Instead, the LFL component was clearly negative, meaning the AOV drop came from within-segment changes.

**This points to internal performance deterioration rather than structural shifts, and leads us to look further into the drivers behind LFL.**

### L2-B: Decomposing LFL — Price vs. Basket Size

We then broke down LFL into two components:

- **Price**: The average unit price of items sold.
- **Basket**: The number of items per order.

The chart titled "AOV Waterfall by Unit Price & Basket Size" shows that the majority of the drop came from unit price, while basket size remained stable or had minimal contribution.

**This confirms that the AOV decline was price-driven, likely due to heavier discounts or pricing strategy shifts in August 2018—not because customers bought fewer items.**

### L3: Drill-Down by Category

To identify where pricing deteriorated the most, we examined the change in unit price for total 62 categories and selected top 15 categories by delta unit price.

The chart reveals significant unit price drops in categories such as:
- `pcs` (computers)
- `telefonia_fixa` (fixed-line phones)
- `construcao_ferramentas` (tools & construction)
- `portateis_casa` (home appliances)

On the other hand, a few categories like `livros_importados` (imported books) actually saw a rise in unit price, but these gains were not enough to offset the overall decline.

**These insights suggest that the GMV loss was concentrated in a few underperforming categories with sharp price declines, which should be prioritized for pricing review or promotional adjustment.**

