## How to use

To update courses, run the notebook and save as .html:
- for multi_courses: change course_list (for all courses on edX), mooc_list (Moocs) and pe_list (professional education) as needed

 ![course_list](images/course_list.png)

- for a specific course: change course_id as needed

 ![course_id](images/course_id.png)

- Cell => Run all

 ![run_all](images/run_all.png)

- FIle => Download as => Html(.html)

 ![html](images/html.png)

If want to present as slideshow:
- multi_courses
~~~
ipython nbconvert multi_courses.ipynb --to slides --template slides_reveal.tpl --post serve
~~~
- a specific course
~~~
ipython nbconvert Climate2015.ipynb --to slides --template slides_reveal.tpl --post serve
~~~
