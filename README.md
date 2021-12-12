# CSYE7200_Team_Project_Team3
Dota 2 Win Prediction


## Team Member:
1.Hanwen Jiang

2.Renqiu Chen



## Project proposal: 

### Some of the background: 
e-sports has developed rapidly these years, millions of people watching the competition. It is not unusual to see many websites let people to simulate either
the pre-game match information or post-game match information, the system will give a most likely result, which called the prediction.

### Introduction: 
we investigate on the pre-game match information, which we predict the result based on the the teams composition on two sides. We analyze the data from kaggle website,
and assume that player all play in a higher rank and each player skills is similar. We try to build the model and make the prediction based on the pre-game team composition.

### Methodolgy and Implementation:
For our backend, we use spark to do the data pre-processing, and extract the feature: synergy, countering and offset. We train 5 different models (DT,GBT,MLPC,RF,SVC) using these features. We use
the model with best accuracy to make the prediction. For web application, we use play framework to implement it. Basically, user will select 10 heroes from the drop_down list, and the
it will passed to the controllers, and the controllers will use the model to predict it and return the result back to the client.



## How to run our project: there are two ways

1.If you want to run the playframework (the final demo of the whole project: includes front-end and back-end), the following are the things you should do:

a. First, you should have spark 3.1.2 version installed on your computer, do not forget to add the "bin" folder to your system path.

b. You should have hadoop 3.1.0 installed on your computer, ***important***: make sure to add "hadoop.dll" and "winutils" to your hadoop bin folder, and for the window user:
also add your "hadoop.dll" file to the windows/system32 folder. Do not forget to add hadoop bin folder to your system path.

c. Open your intellij, make sure you have sbt and scala installed on your system.

d. Open the terminal in intellij, cd PlayFramework---UI, and then type "sbt run" command, open up a browser, go to the localhost at port 9000.

e. Select 10 heroes from the drop_down list, click submit button and server will respond and give the predicting result in a short period of time.

2.If you want to run it without the UI, just run the main program (which located in src/main/scala/edu.neu.csye7200/Main), and type in 10 id numbers in the console (make sure the input must be integers, and range from 1 to 113). From this way, you can see how the model get trained and predicting the result.

