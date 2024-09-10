# World layoffs

### Project Overview

This data analysis project aims to provide insights into the world layoffs over the past year since the pandemic. By analyzing various aspects of the layoff data and to gain a deeper understanding of the data.

### Data Source

The primary dataset used for this analysis is the "layoff.csv" file, containing detailed information about each layoff made by every company over the past year.

### Tools

- SQL Server - For Data Cleaning and Analysis


### Data Cleaning/Preparation

In the initial data preparation phase, i performed the following tasks;
1. Data loading and inspection.
2. Handling missing values and NULL values.
3. Data cleaning by removing duplicates and Formatting.


### Exploratory Data Analysis

EDA involved exploring the layoff data to answer questions, such as;

- What company had the most layoffs?
- What year had the most layoff in each company?
- What industry had most layoff?


### Data Analysis

```sql
SELECT Year(`date`), SUM(total_laid_off) FROM layoffs_staging2
GROUP BY Year(`date`)
ORDER BY 1 DESC;
```

### Results/Findings

The analysis results are summarized as follows;
1. The Companies layoffs has steadily increased over the past year.
2. Industry like Retail had the highest layoffs.

### Limitations

I had to create a new table in my analysis so as not to alter the data set while i do my data cleaning and analysis.i removed NULL values and also updated the missing values.
