import json
import urllib2
import datetime



def homepage_courses():
    """
    Date, position and title of any UBC course on that day;
    If no UBC course on the homepage, then print 'No courses from UBC on the homepage today'.
    """
    today = datetime.date.today()
    url = 'https://www.edx.org/api/discovery/v1/cards?limit=12&tags=14e5ea80-f725-4cd6-82f6-6d2bd63d5159'
    data = json.loads(urllib2.urlopen(url).read())
    with open('/Users/katrinani/Google Drive/Data scripts/homepage_courses.txt', 'a+') as f:
        f.write(str(today) + '\n')
        pos = 1
        ind = True
        for entry in data:
            if entry["attributes"]["course_org"] == "UBCx":
                ind = False
                print 'Pos %s: ' % (pos) + entry['title'].strip()
                f.write('Pos %s: ' % (pos) + entry['title'].strip().encode('utf8') + '\n')
            pos += 1
        if ind:
            print 'No courses from UBC on the homepage today'
            f.write('No courses from UBC on the homepage today\n')
        f.write('\n')



if __name__=="__main__":

    homepage_courses()