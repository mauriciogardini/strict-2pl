# Strict Two-Phase Locking
Strict two-phase locking implemented in Python for the Database Implementation Aspects course. 

The competition control manager works by executing and interpretating 4 different stories, making the deviations and taking the necessary actions to execute all the provided actions, informing at the end of the evaluation of each initial story which was the equivalent final story.

## Stories
These are the stories that will be executed by the program:
- Story without conflicts
  r1[x] r2[y] r1[y] c1 w2[x] c2
- Story which has an operation that needs to be delayed
  r1[x] w1[x] w2[x] c1 c2
- Story with a deadlock
  r1[x] w2[y] r1[y] w2[x] c1 c2
- Story which has an operation that can't be executed
  r1[x] r2[y] r1[y] c1 r1[x] w2[x] c2
