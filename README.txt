/* Query 2 */ 
SELECT DISTINCT zip 
FROM users u 
WHERE 
	u.occupation = 15 
	OR u.occupation = 17;

/* Query 3 */ 
SELECT DISTINCT r.movie 
FROM ratings r, users u 
WHERE r.userid = u.userid 
	AND r.rating = 5 
	AND u.occupation = 6;

/* Query 4 */ 
SELECT m.title, ROUND(AVG(r.rating), 1) 
FROM users u, ratings r, movies m 
WHERE 
	u.userid = r.userid 
	AND r.movieid = m.movieid 
	AND u.age = 35 GROUP BY r.movieid;