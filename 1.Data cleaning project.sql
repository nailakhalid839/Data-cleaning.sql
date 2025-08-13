-- Data Cleaning


-- 1. Remove Duplicates
-- 2. standardize the data
-- 3.Null values or blanks values
-- 4. Remove any columns or rows



select *
from layoffs;


-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens


Create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select*
from layoffs;

-- 1. Remove Duplicates

# First let's check for duplicates


select*
from layoffs_staging;


select*,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;




WITH duplicate_rows AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
         ) AS row_num
  FROM layoffs_staging
)
SELECT *
FROM duplicate_rows
WHERE row_num > 1;

-- let's just look at oda to confirm

select *
from layoffs_staging
where company = 'oda';


select *
from layoffs_staging
where date = '8/5/2021';


WITH duplicate_rows AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
         ) AS row_num
  FROM layoffs_staging
)
delete
FROM duplicate_rows
WHERE row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially


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
  `row_num` Int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



select*
from layoffs_staging2;


insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

select*
from layoffs_staging2;

select*
from layoffs_staging2
where row_num > 1;


DESCRIBE layoffs_staging2;

-- now that we have this we can delete rows were row_num is greater than 2

delete
from layoffs_staging2
where row_num > 1;

select*
from layoffs_staging2;


-- standardizing data

select distinct(company)
from layoffs_staging2;

select company, trim(company)
from layoffs_staging2;


update layoffs_staging2
set company = trim(company);


SELECT * FROM layoffs_staging2;


select distinct(company)
from layoffs_staging2;



select distinct industry
from layoffs_staging2;


select*
from layoffs_staging2
where industry like 'crypto%';

drop table if exists layoffs_staging2;

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';


SET SQL_SAFE_UPDATES = 0;


update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

SHOW DATABASES;

use world_layoffs;


UPDATE layoffs_staging
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry FROM layoffs_staging
WHERE industry LIKE 'crypto%';


select distinct location
from layoffs_staging2
order by 1;


select distinct country
from layoffs_staging2
order by 1;


select* 
from layoffs_staging2
where country like 'united states%';


select distinct country, trim(trailing '.'  from country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country = trim(trailing '.'  from country)
where country like 'united states%';


select `date`
from layoffs_staging2;


select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;


update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;


alter table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2;


select *
from layoffs_staging2
where total_laid_off is null;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


update layoffs_staging2
set industry = null
where industry = '';

select*
from layoffs_staging2
where industry is null
or industry = '';


select *
from layoffs_staging2
where company like 'bally%'; 

select*
from layoffs_staging2
where company = 'airbnb';


select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry =	'')
and t2.industry is not null;



select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry =	'')
and t2.industry is not null;


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where t1.industry is null 
and t2.industry is not null;



update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry =	'')
and t2.industry is not null;


select *
from layoffs_staging2;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2;


alter table layoffs_staging2
drop column row_num;


select *
from layoffs_staging2;

