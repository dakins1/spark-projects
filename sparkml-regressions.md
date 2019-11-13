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

    * Average error of 2.5E-13, so really 0, g/kg. This means our training model is spot on with its predictions. 