TEX=tex
PDFTEX=platex
DVI2PDF=dvipdfmx

NAME=paraliteman

all: $(NAME).pdf

$(NAME).pdf: $(NAME).tex 
	$(TEX) $(NAME).tex
	$(DVI2PDF) $(NAME).dvi

clean:
	-rm -f *.log *.aux *.bbl *.blg
	-rm -f *.dvi *.ps *.out
