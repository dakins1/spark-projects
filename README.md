https://www.kaggle.com/ayushkalla1/rotten-tomatoes-movie-database

This dataset has information about a bunch of movies listed on Rotten Tomatoes. Each data entry is for a movie released before July 2017. Each row contains the movie’s Rotten Tomatoes simple data about the movie, such as studio, cast, and release year. The data is relatively simple and would ideally contain more info about each movie, but it seems there is still enough data present to possibly reach some conclusions. This data does not have the Rotten Tomatoes ratings for each movie, although I don’t think it would be too hard to find another dataset containing the ratings. At the very least, the code used to scrape Rotten Tomatoes’s website is on GitHub, so maybe I’d be able to add in something that scrapes the ratings. 

Questions I would like to ask, assuming I can find the ratings for each movie:
One interest I would have is whether or not audiences/critics have a bias or prejudice against certain studios, actors, directors, or writers when they leave Rotten Tomato reviews. Do reviewers give a movie a positive/negative rating because it has an actor they do/don’t like, regardless of other factors? Or do their ratings consider multiple factors? This will be tricky to answer, because it’s difficult to determine if a good rating comes from a bias or an actually positive reception of the film. For example, if all of Leonardo DiCaprio’s movies have a good rating, is that because a) reviewers are biased and always give his movies a good rating; b) he is just that consistently a good actor, and no matter the other qualities of the film, he is able to compensate so much the film receives a good rating; or c) he consistently lands a role in good movies that are predestined to receive a good rating, even if his performance isn’t that good? I’d like to apply the same questioning to studios, writers, directors, etc. 
	In a similar vein, I’d like to list, in order of importance, the factors of a production. Can the right director always make a good movie regardless of budget and actors? Or can a studio always make a good movie regardless of actors and directors? Going the other direction, which factor can bring down a movie the most? For example, will a low-budget always tank a movie’s success? Or can a bad producer make even the best director and most generous budget useless?
I find my questions about the biases of ratings to be interesting because it might reveal how the film critic industry functions. Everyone loves a movie that film critics despise and hates a movie that film critics praise. Discovering if there are any biases might be able to explain the dichotomy. Perhaps more importantly, though, discovering biases will affect how much I use Rotten Tomatoes to determine if I will spend $10 on a movie or not. 
I find these questions interesting because they are related to the Auteur Theory in film studies. Film scholars debate whether a movie is the artistic product of a director, producer, or studio. For example, every Marvel movie is very similar, but each one has a different director and producer. On the other hand, all of Quentin Tarantino’s movies are similar, but he has made them across different studios and producers. I don’t think any amount of data could answer this question definitively, although I do think analysis on a large dataset could offer some suggestions or pose new arguments. 
	Aside from the pursuit of knowledge, answers to these questions could be very helpful for Hollywood. If a studio finds out the director does not matter as much a producer, they can skimp on their director budget and put it towards other, more important factors. While I don’t expect these answers to be groundbreaking, and they probably won’t affect a century-old industry, again, it might shed a little bit of light on a complex problem. 

End of semester review: 

You might be able to answer some really simple questions from this, but I don't think you'd be able to make sweeping claims. A good place to start would be to collect every producer and calculate how consistent their movies' ratings are. Then do the same for director, writer, studio, etc. If any role has an especially high consistency, you could further explore how that role has affected its movies' ratings, both in good and bad ways. I would then see what the data shows to decide what question to ask next. I also think it would be important for your results to be very consistent. E.g., it can't be only a few producers who had high consistency; almost every producer would need to have high consistency, if you wanted to make a strong claim. 

I think you're unlikely to find anything meaningful, though. There are simply too many factors to be able to pinpoint what makes a movie good.
  
  
https://fragilefamilies.princeton.edu/data-and-documentation/data-contents-overview
You need permission to see the data, but this link will give you the overview. If you want permission, it’s super easy to request: https://opr.princeton.edu/archive/restricted/Default.aspx My request was approved in less than 10 minutes. 

Fragile Families and Child Wellbeing Study

This dataset was produced by a study that found ~5,000 children born from 1998-2000 and conducted interviews with the children and their families. Each family was interviewed when their child was 0, 1, 3, 5, 9, and 15 years old. The study hoped to collect data that would reveal what factors of a child’s life are the most influential in the child’s wellbeing and life choices. Each “wave” of the interview collects a lot of data, like the condition of the house and neighborhood of the family, household income, health of the child, sexual activity of the child, and drug/substance use of the child, to name a few.

Questions I’d like to ask:
	The overall focus would be: how likely will the environment affect a child’s lifestyle choices? If so, what are the biggest factors in a child’s lifestyle choices and wellbeing? This might be answered with a list of subquestions:
How much does household income affect the child’s health?
How much does household income affect the child’s sexual activity
How much does household income affect the child’s substance usage?
All of those questions, but weighing how much the house and neighborhood condition weighs in → which in turn helps answer, how much of a person’s choices are the result of their environment?
Rinse and repeat with every variable
Seeing the likelihood of one variable predicting another would be the first step. After that, I’d like to start looking at conflicting circumstances, e.g. find children who have similar drug and sex choices but completely different household incomes, then see what the difference between the two groups are. This might reveal that household income isn’t a true cause for “rougher” lifestyles and choices but instead is just a correlation.

The nature of home environments and lifestyle choices research is, in my opinion, rather nebulous. It’s hard to prove anything or conduct studies on this kind of stuff. Therefore, I think a significant amount of the value in this dataset would probably be found through learning associations. This would, theoretically, reveal trends between environments and life outcomes. 

My initial interest in this dataset came from my girlfriend, who used this dataset for her undergraduate honors thesis at UT. I picked it because I knew it was a solid dataset. After looking at it some more, though, I now think machine learning could do a lot with this dataset. If my questions could be answered, they would provide valuable insights to familial psychology. Fun fact, they will conduct another wave of interviews in 2020. The focal children will be 22 by then. Questions will be about “socioeconomic status, family formation, physical and mental health, family relationships and social support, local area contexts, and access to and participation in health care, higher education, housing, and other government programs.”

See my final write up for my answer to this :)

https://opr.princeton.edu/archive/THEOP/
This is from the same source as the Fragile Families data. So you still need to request permission to see the data, but that link will give you the overview. 

This dataset has two chunks. The first is from nine Texas colleges, two of which are private (Rice and SMU. Too bad they didn’t pick Trinity, you might have been included.). The data collected is about enrollment statistics, like the demographics, percent economically disadvantaged, and type of highschools of the enrollees. This set also includes all the transcripts of the enrollees. The second chunk is data from two cohorts of high school students, one cohort a senior class at the time of the survey, the other a sophomore class. Each cohort was spread across 105 randomly chosen high schools. Both cohorts answered questions about their familiarity with college and admission processes, their college plans, and then some background info, like ethnicity. A subset of each cohort was reinterviewed. The seniors resurveyed one year after graduation, the sophomores resurveyed once they were seniors. The purpose of the second wave of surveys was to gauge how the cohorts had changed over time. The most important aspect of this data set is that its collection started on or near 1998, the year Texas passed a law saying the top 10% of any high school class gets automatic acceptance into a public university. 

I’d be interested to see if this dataset reveals how the 10% law changed admission and enrollment trends in universities. Did this law:
Lower the academic performance of Texas universities?
Lower the academic standard of Texas universities? E.g. perhaps UT Austin loses some prestige from losing student body selectiveness
Draw students away from private universities?
Give a higher-education opportunity to less privileged students, who would not have considered or been able to attend college without the law?
In a similar vein, did the law “spread” higher education throughout Texas more? E.g. did more small towns see an increase in college education? (This might need a supplemental data set)
Increase the diversity at universities?
How did private universities change their enrollment policies once this law took effect?
How did public universities change their enrollment policies?
What universities were not affected by this law? Does that mean they were already accepting a similar spread of students?
Overall, was the 10% law a good or bad decision?

I think most of the analysis will come from the college datasets, although the student dataset may help complement the findings. 

Originally I was hoping to find a dataset that contained thousands of college essays and data about if the application with that essay was accepted, rejected, or waitlisted. I thought it’d be cool to see what words, moods, or tones got accepted. I also thought it’d be interesting if there were trends in what the students wrote about. But I quickly realized this dataset might never exist, since application essays are too personal to be released like that. So this was the next best thing. I still think this dataset will be really interesting, though, because since applying to college I’ve wondered about what the 10% law has accomplished in Texas. And since college admissions is such a mysterious science, I think it will be interesting to see how this law might have affected enrollment. The 10% law is also, in my opinion, a really important law. For most of highschool it was my main goal to be within UT's % threshold for acceptance, so I know what it's like to be affected by this law. Any insight into a law that affects so many futures will be valuable. 

You'd probably be able to answer most of these questions. They are straightforward questions that simple statistics would be able to demonstrate. But given my experience with the Fragile Families dataset, I would try to use other datasets first to answer these questions. This one is pretty small and has only a handful of colleges, whereas you can probably find more general but much more representative datasets that contain similar data. It also probably suffers from the same problem of too-specific questions and missing participants in future waves. 

If you had enough data from other datasets before and after the 1998 law, you could probably get to some really interesting conclusions. Pairing this with geographic data would be really interesting. Color coding data points based on their college experience, then showing how those colors changed over the years would provide a good idea of how college admissions were affected by the law.  

Once I had experimented with bigger datasets, I think I would turn to this dataset to answer more specific questions. But only if the previous analyses made me curious about a specific question. 