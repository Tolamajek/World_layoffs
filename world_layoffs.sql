SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_CTE as (SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Yahoo';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

DELETE 
FROM layoffs_staging2
WHERE row_num >1;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT industry, TRIM(industry)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = TRIM(industry);

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry ='';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT st1.company,st1.industry,st2.industry
FROM layoffs_staging2 st1
JOIN layoffs_staging2 st2
ON st1.company = st2.company
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;

UPDATE layoffs_staging2 st1
JOIN layoffs_staging2 st2
ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE (st1.industry IS NULL )
AND st2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT company, MAX(total_laid_off) AS max_laid_off
 FROM layoffs_staging2
 GROUP BY company
 ORDER BY max_laid_off desc
 LIMIT 1 ;
 
SELECT company, MAX(percentage_laid_off) AS max_p_laid_off
 FROM layoffs_staging2
 GROUP BY company
 ORDER BY max_p_laid_off desc
 LIMIT 1 ;
 
 SELECT industry, SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY industry
 ORDER BY 2 DESC;
 
 SELECT MIN(`date`), MAX(`date`)
 FROM layoffs_staging2;
 
 SELECT industry, SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY industry
 ORDER BY 2 DESC;
 
 SELECT YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY YEAR(`date`)
 ORDER BY 1 DESC;
 
 SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
 FROM layoffs_staging2
 WHERE SUBSTRING(`date`,1,7) IS NOT NULL
  GROUP BY `MONTH`
 ORDER BY 1 ASC;
 
 WITH rolling_CTE AS
 (
 SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
 FROM layoffs_staging2
 WHERE SUBSTRING(`date`,1,7) IS NOT NULL
  GROUP BY `MONTH`
 ORDER BY 1 ASC)
 SELECT `MONTH`,total_off,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
 FROM rolling_CTE;
 
 SELECT company, YEAR(`date`),SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ORDER BY 3 DESC;
 
 WITH company_year (company, years,total_laid_off) AS 
 (SELECT company, YEAR(`date`),SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ),company_year_rank AS
 (SELECT *,
 DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking 
 FROM company_year
 WHERE years IS NOT NULL)
 SELECT*
 FROM company_year_rank
 WHERE Ranking <= 5
 ; 
 SELECT industry, YEAR(`date`),SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY industry, YEAR(`date`)
 ORDER BY 3 DESC;