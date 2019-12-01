1. County level codes? How many entries?

    * Codes 70-78 are used for county-related data. 
    * 11470048 of these bad boys are county-related.
    * 78 has 3984876
    * 76 has 2162440
    * 72 has 76460
    * 77 has 3396052
    * 73 has 277932
    * 70 has 13084
    * 75 has 1100372
    * 71 has 51964
    * 74 has 406868

2. How many are for Bexar County?

    * 9244 entries for Bexar County

3. Three most common industry codes, number records for each 

    * Code 10 has 76952 entries
    * Code 102 has 54360 entries
    * Code 1025 has 40588 entries

4. Top wages

    * Elementary and secondary schools had $244321853090 in wages
    * Managing offices had $219387605630 in wages
    * General medical and surgical hospitals had $205806016743 in wages

5. Clustering

![aeioaj](src/main/scala/sparkml/qtrlywages.png)

This is from clustering on the average total_qtrly_wages and average month2_emplvl from the fourth quarter of a whole county, i.e. each county has only one data point. I chose month2 and the fourth quarter so we have the unemployment level from November. For the graph, I put per_dem on the x axis to see how well the clusters line up. I arbitrarily chose wages for the y-axis. Seeing that there is no clear break between the red and blue dots on the x axis, I would not find total wages and unemployment effective clustering variables. It is interesting, though, how most of the blue points are past 50% democratic and above $50000000 wages. 

