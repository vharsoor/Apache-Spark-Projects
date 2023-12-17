In this project, it was required to do spatial hot spot analysis. I had to complete two different hot spot analysis tasks.<br/>

**1. Hot zone analysis**<br/>
This task includes performing a range join operation on a rectangle dataset and a point dataset. For each rectangle, the number of points located within the rectangle is obtained. The hotter rectangle means that it includes more points. So, this task is to calculate the hotness of all the rectangles.<br/>

**2. Hot cell analysis**<br/>
This task will focus on applying spatial statistics to spatial-temporal big data to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.<br/>
The Problem Definition page is here: http://sigspatial2016.sigspatial.org/giscup2016/problemLinks to an external site.<br/>
The Submit Format page is here: http://sigspatial2016.sigspatial.org/giscup2016/submitLinks to an external site.<br/>
Special requirement (different from GIS CUP)<br/>
As stated in the Problem Definition page, in this task, I was asked to implement a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. We call it "Hot cell analysis”.<br/>
To reduce the computation power need, these following changes were made:
1.	The input will be a monthly taxi trip dataset from 2009 - 2012. For example, "yellow_tripdata_2009-01_point.csv", "yellow_tripdata_2010-02_point.csv".
2.	Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees.
3.	I had to use 1 day as the Time Step size. The first day of a month is step 1. Every month has 31 days.
4.	I only had to consider Pick-up Location.

