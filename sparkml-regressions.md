1. How many columns in bottle.csv have more than half the rows non-null? 
    * 36 columns

2. Average bottle count per cast?
    * 25 bottles per cast

3. Plot of casts

Blue represents 0C and red represents 12C

![aiwejof](src/main/scala/sparkml/casts.png)

4. Linear Regression of temperature to salinity

    * Average error of 0.279 g/kg

5. Linear regression of temperature, depth, and O2ml_L to salinity

    * Average error of .139 g/kg. 

6. Linear regression with columns of our choosing to predict O2ml_L

I chose to use latitude, longitude and the Julian day to predict O2ml_L. My thinking was that the location and the time of the year would be good indicators, as the season and distance from the shore can affect the water. This gave an average error of 1.849. The minimum O2ml_L value is 0.0, and the maximum is 11.13. 1.849 is probably too big of an average error if you're really wanting to predict O2ml_L. 