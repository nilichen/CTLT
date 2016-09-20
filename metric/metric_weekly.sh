cd /Users/katrinani/Documents/CTLT/analysis/metric
jupyter nbconvert --to html --execute multi_courses.ipynb --ExecutePreprocessor.timeout=500
mv multi_courses.html '/Users/katrinani/Google Drive/Data scripts/metrics'
