Repository klonen entweder per SSH (mit github-Account und ggf. Key) oder per HTTPS:

git clone https://github.com/uschnoor/ROCED.git
git clone git@github.com:uschnoor/ROCED.git

cd ROCED/publication/Draft_CompSoftwBigScience

pdflatex nemo-virtualization.tex


Änderungen im Text vornehmen, dann einen Branch machen und diesen per Pull request einfügen:

- git status
- git checkout -b <Branchname>
- git add <file>
- git commit -m "what you have changed"
- git push origin <Branchname>

Pull request im Webinterface machen:
- Auf den Branch wechseln
- "Compare & Pull request"
- base fork sollte für die Publikation "uschnoor/ROCED" sein
- Änderungen durchschauen, Create Pull Request

