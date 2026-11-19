\NeedsTeXFormat{LaTeX2e}[1995/12/01]
\ProvidesClass{SSA-SRL}
              [2024/12/11 v1.5n
 Standard LaTeX document class]

\newcommand\@ptsize{}
\newif\iffinals\global\finalsfalse
\newif\iflettersize\global\lettersizetrue%
\newif\iflegalsize\global\legalsizefalse%
\newif\iftabloidsize\global\tabloidsizefalse%
\newif\ifcolorimage\global\colorimagefalse%
\newif\ifFoottext\global\Foottextfalse
\newif\ifpddata\global\pddatafalse
\newif\ifAbstract\global\Abstractfalse
\newif\ifnoauthor\global\noauthorfalse%
\newif\ifOAarticle\global\OAarticlefalse
\newif\ifmultiau\global\multiaufalse
\newif\ifaugroup\global\augroupfalse
\newif\iftwocolcaption\global\twocolcaptionfalse
\newif\ifrightcaption\global\rightcaptionfalse
\newif\ifdbleed\global\dbleedfalse%
\newif\ifnarrowcol\global\narrowcolfalse
\newif\iftablefootnotenone\tablefootnotenonefalse
\newif\ifunnumtable\unnumtablefalse
\newif\ifsidewaystable\sidewaystablefalse%
\newif\iftwocoltable\global\twocoltablefalse
\newif\if@LDdoublespace\global\@LDdoublespacefalse
\newif\ifLineNum\global\LineNumfalse
\newif\ifmetarunon\global\metarunonfalse
\newif\ifswitchcols

\@twosidetrue\@mparswitchtrue

\newcount\tempcount
\newcount\affcount\affcount0%
\newcount\afftempcount
\newcount\tempaffcount
\newcount\instcount\instcount0%
\newcount\insttempcount
\newcount\tempinstcount
\newcount\deptcount\deptcount0%
\newcount\depttempcount
\newcount\tempdeptcount
\newcount\streetcount\streetcount0%
\newcount\streettempcount
\newcount\tempstreetcount
\newcount\citycount\citycount0%
\newcount\citytempcount
\newcount\tempcitycount
\newcount\statecount\statecount0%
\newcount\statetempcount
\newcount\tempstatecount
\newcount\postcodecount\postcodecount0%
\newcount\postcodetempcount
\newcount\temppostcodecount
\newcount\countrycount\countrycount0%
\newcount\countrytempcount
\newcount\tempcountrycount
\newcount\refvaluecount
\newcount\temprefvaluecount
\newcount\refvaluetempcount
\newcount\enunheadcount
\newcount\tempenunheadcount
\newcount\enuntempcount
\newcount\algheadcount
\newcount\tempalgheadcount
\newcount\algtempcount
\newcount\authorcount\authorcount0%
\newcount\xrefcount\xrefcount0%
\newcount\tempauthorcount\tempauthorcount0%
\newcount\tempxrefcount\tempxrefcount0%
\newcount\authorcommacount\authorcommacount0%
\newcount\xrefcommacount\xrefcommacount0%
\newcount\authortempcount\authortempcount0%
\newcount\xreftempcount\xreftempcount0%
\newcount\keywordcount
\newcount\keytempcount
\newcount\tempkeywordcount
\newcount\keyiiwordcount
\newcount\keyiitempcount
\newcount\tempkeyiiwordcount
\newcount\minute
\newcount\hour
\newcount\authorcount \authorcount0%
\newcount\xrefcount \xrefcount0%
\newcount\tempaffilcount
\newcount\authorcount \authorcount0%
\newcount\xrefcount \xrefcount0%
\newcount\tempauthorcount \tempauthorcount0%
\newcount\tempxrefcount \tempxrefcount0%
\newcount\affiliationcount \affiliationcount0%
\newcount\tempaffiliationcount \tempaffiliationcount0%
\newcount\tempcount
\newcount\affilcount
\newcount\affiltempcount
\newcount\tempaffilcount
\newcount\membercount
\newcount\membertempcount
\newcount\tempmembercount
\newcount\historycount
\newcount\historytempcount
\newcount\temphistorycount
\newcount\historycount
\newcount\historytempcount
\newcount\temphistorycount
\newcount\symtempcount
\newcount\tfootcount\global\tfootcount0
\newcount\temptfootcount%
\newcount\tbodycount\global\tbodycount0
\newcount\tgroupcount\tgroupcount0
\newcount\bcount
\newcount\ncount\ncount=1
\newcount\ccount\ccount=1
\newcount\acomcount
\newcount\tempacomcount
\newcount\acomtempcount
\newcount\TotalCharCount\TotalCharCount0
\newcount\auquerycount\auquerycount0%
\newcount\querycheckcount\querycheckcount0%
\newcount\tempauquerycount\tempauquerycount0%
\newcount\seccount%
\newcount\authcount%
\newcount\SCOUNT
\newcount\HCOUNT
\newcount\fpcount
\newcount\viper@penalty
\newcount\AQpage\AQpage0%
\newcount\AQlpage\AQlpage0%
\newcount\orcidaucnt
%%%%%%%%%%%%%%% B-BookTabs %%%%%%%%%%%%%%
\newcount\@cmidla
\newcount\@cmidlb
\newdimen\@aboverulesep
\newdimen\@belowrulesep
\newcount\@thisruleclass
\newcount\@lastruleclass
%%%%%%%%%%%%%%% E-BookTabs %%%%%%%%%%%%%%

\newcounter{theorem}
% \newcounter{afrfilnote}
% \newcounter{r@tfl@t}

\newdimen\trimwidthval
\newdimen\trimwidthbleedval
\newdimen\trimheightval
\newdimen\trimheightbleedval
\newdimen\normaltextheight%
\newdimen\blindfoliodrop%
%\newdimen\LabelSep \LabelSep4.7pt
\newdimen\leftmarginvii
\newdimen\leftmarginviii
\newdimen\leftmarginix
\newdimen\leftmarginx
\newdimen\toprulewidth
\newdimen\midrulewidth
\newdimen\botrulewidth
\newdimen\cmidrulewidth
\newdimen\belowbottomsep
\newdimen\abovetopsep
\newdimen\cmidrulesep
\newdimen\cmidrulekern
\newdimen\defaultaddspace
\newdimen\foot@parindent
\newdimen\bibindent
\newdimen\letterwidth%
\newdimen\letterheight%
\newdimen\legalwidth%
\newdimen\legalheight%
\newdimen\tabloidwidth%
\newdimen\tabloidheight%
\newdimen\mathindent%
\newdimen\aboverulesep
\newdimen\@aboverulesep
\newdimen\@belowrulesep
\newdimen\belowrulesep
\newdimen\@thisrulewidth
\newdimen\tablewidth
\newdimen\tabledim%
\newdimen\@partialpageht
\newdimen\storedboxheight

\newbox\abstractbox%
\newbox\figcapbox
\newbox\tempbox%
\newbox\temptbox
\newbox\ctabox

\newskip\normalbaselineskip%
\newskip\colmargin
\newskip\texttypewidthsep
\newskip\authorbelowskip
\newskip\topsepi
\newskip\topsepii
\newskip\topsepiii
\newskip\topsepiv
\newskip\topsepv
\newskip\topsepvi
\newskip\topsepvii
\newskip\topsepviii
\newskip\topsepix
\newskip\topsepx
\newskip\itemsepi
\newskip\itemsepii
\newskip\itemsepiii
\newskip\itemsepiv
\newskip\itemsepv
\newskip\itemsepvi
\newskip\itemsepvii
\newskip\itemsepviii
\newskip\itemsepix
\newskip\itemsepx
\newskip\fignumcapsep\fignumcapsep=2.5pt
\newskip\figcapdescsep\figcapdescsep=0pt
\newskip\tablenumcapsep
\newskip\tableleftskip%
\newskip\tablerightskip%
\newskip\tabnoteleftskip%
\newskip\tabnoterightskip%

\newlength\subtitleval
\newlength\abovecaptionskip
\newlength\belowcaptionskip
\newlength\trimwidth%
\newlength\trimheight%
\newlength\typewidth%
\newlength\typeheight%
%
\newtoks\authorrunning
\newtoks\titlerunning
\newtoks\tocauthor
\newtoks\toctitle
\newtoks\CharCounttoks

\newwrite\@maintab%%
\newwrite\@mainndx
\newwrite\@querywrite%

\renewcommand\@ptsize{0}

\DeclareOption{dbleed}{\global\dbleedtrue}
\DeclareOption{finals}{\global\finalstrue\global\noquerytrue\global\proofstagefalse}
\DeclareOption{draft}{\setlength\overfullrule{0pt}}
\DeclareOption{onecolumn}{\@twocolumnfalse}
\DeclareOption{twocolumn}{\@twocolumntrue}
\DeclareOption{query}{\global\noqueryfalse}
\DeclareOption{final}{\setlength\overfullrule{0pt}}
\DeclareOption{OA}{\global\OAarticletrue}
\DeclareOption{linenum}{\global\LineNumtrue}
\DeclareOption{metarunon}{\global\metarunontrue}

\DeclareOption{doublespace}{\global\@LDdoublespacetrue}


\DeclareOption{leqno}{\input{leqno.clo}}
\DeclareOption{fleqn}{\input{fleqn.clo}}

\ExecuteOptions{final,twocolumn}
\ProcessOptions

\RequirePackage{xifthen}
\RequirePackage{ifpdf}
\RequirePackage{etex}
\RequirePackage{amsthm}
\RequirePackage{soul}
\RequirePackage{calc}

\renewcommand*\rmdefault{ptm}
\renewcommand*\sfdefault{phv}
\renewcommand*\ttdefault{pcr}

\RequirePackage[utf8]{inputenc}
\RequirePackage[T1]{fontenc}
\RequirePackage{mathptmx}

\RequirePackage{marvosym}
\RequirePackage{stix2}
\RequirePackage{mathtools}

\DeclareMathSizes{10}{10}{7}{6}

\RequirePackage{amsbsy}[1995/01/20]
%\RequirePackage{amssymb}  % commented while stix2 is already loaded
\RequirePackage{graphicx}
\RequirePackage{ifthen}
\RequirePackage{xcolor}

\RequirePackage{ifsym}

\RequirePackage{makeidx}
\RequirePackage{ifthen}
\RequirePackage{calc}
\RequirePackage{multicol}
\RequirePackage{algorithm}
\RequirePackage{stfloats}
\RequirePackage{wrapfig}
\RequirePackage{lipsum}
\RequirePackage{cuted}
\RequirePackage{url}
\RequirePackage{etoolbox}
\RequirePackage{eurosym}
\RequirePackage{framed}

\RequirePackage{algorithm}
\RequirePackage[noend]{algpseudocode}

\RequirePackage{lastpage}  % Number of pages in the document

\def\etal{\itshape et~al.}

\if@LDdoublespace
\AtBeginDocument{\RequirePackage{setspace}
\doublespacing}
\else\fi

\ifLineNum
%\AtBeginDocument{%
\if@twocolumn
\RequirePackage[switch]{lineno}
\else
\RequirePackage{lineno}
\fi
\linenumbers%
%}
\else\fi

\RequirePackage[figuresright]{rotating}

% \RequirePackage{breakurl} % not required for pdflatex compilation

% \def\@listi{\leftmargin\leftmargini
%             \labelwidth\leftmargini
%             \advance\labelwidth-\labelsep
%             \parsep 0\p@%
%             \topsep \topsepi
%             \itemsep\itemsepi}%

% Set document color scheme
\definecolor{color0}{RGB}{107, 110, 105} %42,43,41} % graylike
\definecolor{color1}{RGB}{110, 150, 161} %43,59,63} % author email, doi  0.168, 0.231, 0.247
\definecolor{color2}{RGB}{232, 227, 212} %91,89,83} % abs back
%\definecolor{color2}{RGB}{232, 212, 135} %91,83,53} % abs back
\definecolor{color3}{RGB}{148, 94, 56} %58,37,22} % abs & subsec head
\definecolor{color4}{RGB}{0, 0, 250} %0,0,98} % blue for hyperlinks
\definecolor{color5}{RGB}{173, 166, 125} %0,0,98} % blue for hyperlinks
\definecolor{color6}{RGB}{204, 204, 204} %80,80,80
\definecolor{color7}{RGB}{217, 217, 217}  %85,85,85
\definecolor{shadecolor}{RGB}{232, 227, 212}
%\definecolor{color4}{RGB}{00,00,00} % dummy
% color2 defined by the specific journal style file

\renewcommand\@biblabel[1]{{#1.}} % Remove brackets from label
\renewcommand{\eqref}[1]{\textup{{\normalfont Eq.~(\ref{#1}}\normalfont)}}

%
% \newcommand\today{\ifcase\month\or
%   January\or February\or March\or April\or May\or June\or
%   July\or August\or September\or October\or November\or December\fi
%   \space\number\day, \number\year}
% %
%
% \def\currenttime{%
%      \minute\time
%      \hour\minute
%      \divide\hour60
%      \the\hour:\multiply\hour60\advance\minute-\hour\the\minute}
% %
% %\def\jobtag{\@currname\ {\bf\uppercase{Sample Elements for Quick Reference}}}%%
% \AtBeginDocument{
% \def\jobtag{{\bf LaTeX Sample Elements for Quick Reference}}%%
% \def\@pushfilename{%
%   \xdef\@currnamestack{%
%     {\@currname}%
%     {\@currext}%
%     {\the\catcode`\@}%
%     \@currnamestack}}%
% \@onlypreamble\@pushfilename%
% \@pushfilename%
% }

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Fonts %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Credit Line font
\def\psplainfont{\fontsize{8}{10}\fontfamily{\sfdefault}\selectfont\rightskip0pt plus1fill}
%% headings
\def\rhfont{\fontsize{10}{12}\selectfont\itshape}
\def\foliofont{\fontsize{10}{10}\fontfamily{\sfdefault}\selectfont}
\def\dropfoliofont{\foliofont}
\def\jvolfont{\fontfamily{\sfdefault}\fontsize{10}{10}\bfseries\selectfont}
%% part
\def\PNfont{\fontsize{50}{50}\bfseries\selectfont\leftskip0pt plus1fill\rightskip-10pc plus1fill}
\def\PTfont{\fontsize{32}{38}\bfseries\selectfont\leftskip0pt plus1fill\rightskip-10pc plus1fill}%\leftskip.5pc plus1fill\rightskip.5pc plus1fill}
\def\PSTfont{\fontsize{13}{15}\bfseries\selectfont}%\leftskip.5pc plus1fill\rightskip.5pc plus1fill}
%% chapter
\def\CNfont{\fontfamily{\sfdefault}\fontsize{65}{12}\selectfont\bfseries}
\def\CTfont{\fontfamily{\sfdefault}\fontsize{16}{18}\selectfont\bfseries}
\def\CSTfont{}
\def\abstractfont{\fontfamily{\sfdefault}\fontseries{b}\fontsize{9}{13}\selectfont\mathversion{bold}}
\def\continuedfont{\fontfamily{\sfdefault}\fontsize{8}{8}\itshape\selectfont}
%% section
\def\sectionfont{\fontfamily{\sfdefault}\fontseries{b}\fontsize{12}{14}\selectfont\rightskip0pt plus1fill} %\normalspaceskip}
%\def\ACKsectionfont{\fontfamily{\sfdefault}\fontseries{b}\fontsize{8}{10}\selectfont\rightskip0pt plus1fill} %\normalspaceskip}
\def\subsectionfont{\fontfamily{\sfdefault}\fontsize{10.5}{13}\selectfont\rightskip0pt plus1fill} %\normalspaceskip}
\def\subsubsectionfont{\fontfamily{\sfdefault}\fontsize{9}{11}\selectfont\rightskip0pt plus1fill}
\def\paragraphfont{\fontfamily{\sfdefault}\fontsize{10}{12}\selectfont\itshape}%
\def\subparagraphfont{}%
\def\subsubparagraphfont{}%
%
\def\sectionnumfont{\fontsize{12}{14}\bfseries\selectfont}
\def\subsectionnumfont{\fontsize{11}{13}\bfseries\selectfont}
%
%% figure
\def\figcaptionfont{\fontfamily{\sfdefault}\fontsize{9}{11}\selectfont\raggedright}
\def\figcaptiondescfont{\fontfamily{\sfdefault}\fontsize{9}{11}\bfseries\selectfont}
\def\figcaptionnumfont{\fontfamily{\sfdefault}\fontsize{9}{11}\selectfont\bfseries}
%% TABLE
\def\multilistfont{\fontsize{8.5}{10}\selectfont\leftskip\tableleftskip\rightskip\tablerightskip}% use plus 1fill if needed
\def\multilistheadfont{\fontsize{8.5}{10}\bfseries\selectfont\leftskip\tableleftskip\rightskip\tablerightskip}% use plus 1fill if needed
\def\tablefont{\fontfamily{\sfdefault}\fontsize{8}{11}\selectfont}%\leftskip\tableleftskip\rightskip\tablerightskip}% use plus 1fill if needed
%\def\tabfont{}
\def\tablecaptionfont{\fontfamily{\sfdefault}\fontsize{9}{11}\selectfont\rightskip12pt plus1fill}%\normalspaceskip}% \bfseries
% \def\mathsans#1{{\textsf{\itshape #1}}}
\def\tablesidecaptionfont{\fontsize{8}{10}\selectfont}%Updated
\def\tablecaptionnumfont{\fontfamily{\sfdefault}\fontsize{9}{11}\selectfont}%
\def\TCHfont{\fontsize{10}{12}\selectfont}%
\def\TSCHfont{}%
\def\tabnotefont{\fontsize{8}{10}\selectfont}% use plus 1fill if needed
%
\def\listfont{}
\def\listdevicefont{}
\def\extractfont{\leftskip2pc\rightskip\leftskip}
%
\def\historyfont{\fontencoding{T1}\fontfamily{\sfdefault}\fontseries{m}\fontsize{8}{10}\selectfont\raggedright\itshape} %\normalspaceskip}
\def\titlefont{\fontfamily{\sfdefault}\fontseries{b}\fontsize{21}{27}\selectfont\rightskip84pt plus2fill} %\normalspaceskip} % \global\TitleMicroSymtrue
\def\authorfont{\fontfamily{\sfdefault}\fontsize{11}{13}\selectfont} %\normalspaceskip}
\def\corresfont{\fontencoding{T1}\fontfamily{\sfdefault}\fontseries{m}\fontsize{8}{10}\selectfont\itshape}
\def\subtitlefont{\fontsize{10}{12}\selectfont\itshape\centering}
\def\keyfont{\fontfamily{\sfdefault}\fontsize{8}{12}\selectfont} %\leftskip24pt\rightskip24pt}
\def\keyheadfont{\fontfamily{\sfdefault}\fontseries{b}\fontshape{it}\fontsize{8}{8}\selectfont}
\def\acknowfont{\fontsize{9}{11}\selectfont}
\def\headifont{\fontsize{10}{12}\fontfamily{\sfdefault}\selectfont\leftskip0pt plus1fill\rightskip0pt plus1fill}
\def\refheadfont#1{#1}
\def\affilfont{\fontencoding{T1}\fontfamily{\sfdefault}\fontseries{m}\fontsize{8}{10}\selectfont\raggedright\itshape}
\def\Affilfont{\reset@font{}\fontencoding{T1}\fontfamily{\sfdefault}\fontseries{m}\fontsize{8}{10}\selectfont\raggedright\itshape}
\def\emailfont{\fontencoding{T1}\fontfamily{\sfdefault}\fontseries{m}\fontsize{8}{10}\selectfont\raggedright\itshape}
%

\def\defleftbarcolor{\special{color push cmyk 0 0 0 .2}}%
\def\Black{\special{color push cmyk 0 0 0 1}}%
\newenvironment{absleftbar}{%
\setlength{\fboxsep}{0pt}%
  \def\FrameCommand{{\textcolor{color6}{%
\vrule width 3pt}}\hspace*{9pt}}%
  \MakeFramed {\advance\hsize-\width \FrameRestore}}%
 {\endMakeFramed}


\renewcommand\normalsize{%
   \@setfontsize\normalsize{10}{12.94}%
   \abovedisplayskip8\p@ plus1pt minus1pt
   \abovedisplayshortskip\abovedisplayskip
   \belowdisplayshortskip\abovedisplayskip
   \belowdisplayskip \abovedisplayskip
   \let\@listi\@listI}
\def\biggg#1{{\hbox{$\left#1\vbox to20.5\p@{}\right.\n@space$}}}
\def\Biggg#1{{\hbox{$\left#1\vbox to23.5\p@{}\right.\n@space$}}}
\normalsize
%
\newcommand\small{%
   \@setfontsize\small{8}{10}%
   \abovedisplayskip 8\p@ \@plus3\p@ \@minus4\p@
   \abovedisplayshortskip \z@ \@plus2\p@
   \belowdisplayshortskip 4\p@ \@plus2\p@ \@minus2\p@
   \def\@listi{\leftmargin\leftmargini
               \topsep 4\p@ \@plus2\p@ \@minus2\p@
               \parsep 2\p@ \@plus\p@ \@minus\p@
               \itemsep \parsep}%
   \belowdisplayskip \abovedisplayskip
   \setSmallDelims}
%
\def\setSmallDelims{%
\def\big##1{{\hbox{$\left##1\vbox to7.5\p@{}\right.\n@space$}}}%
\def\Big##1{{\hbox{$\left##1\vbox to10.5\p@{}\right.\n@space$}}}%
\def\bigg##1{{\hbox{$\left##1\vbox to13.5\p@{}\right.\n@space$}}}%
\def\Bigg##1{{\hbox{$\left##1\vbox to16.5\p@{}\right.\n@space$}}}%
\def\biggg##1{{\hbox{$\left##1\vbox to19.5\p@{}\right.\n@space$}}}%
\def\Biggg##1{{\hbox{$\left##1\vbox to22.5\p@{}\right.\n@space$}}}%
}
%
\newcommand\footnotesize{%
   \@setfontsize\footnotesize\@ixpt{11}%
   \abovedisplayskip 6\p@ \@plus2\p@ \@minus4\p@
   \abovedisplayshortskip \z@ \@plus\p@
   \belowdisplayshortskip 3\p@ \@plus\p@ \@minus2\p@
   \def\@listi{\leftmargin\leftmargini
               \topsep 3\p@ \@plus\p@ \@minus\p@
               \parsep 2\p@ \@plus\p@ \@minus\p@
               \itemsep \parsep}%
   \belowdisplayskip \abovedisplayskip
   \setFootnotesizeDelims}
%
\def\setFootnotesizeDelims{%
\def\big##1{{\hbox{$\left##1\vbox to6.5\p@{}\right.\n@space$}}}%
\def\Big##1{{\hbox{$\left##1\vbox to9.5\p@{}\right.\n@space$}}}%
\def\bigg##1{{\hbox{$\left##1\vbox to12.5\p@{}\right.\n@space$}}}%
\def\Bigg##1{{\hbox{$\left##1\vbox to15.5\p@{}\right.\n@space$}}}%
\def\biggg##1{{\hbox{$\left##1\vbox to18.5\p@{}\right.\n@space$}}}%
\def\Biggg##1{{\hbox{$\left##1\vbox to21.5\p@{}\right.\n@space$}}}%
}
%
\newcommand\scriptsize{\@setfontsize\scriptsize\@viipt\@viiipt}
\newcommand\tiny{\@setfontsize\tiny\@vpt\@vipt}
\newcommand\large{\@setfontsize\large\@xiipt{14}}
\newcommand\Large{\@setfontsize\Large\@xivpt{18}}
\newcommand\LARGE{\@setfontsize\LARGE\@xviipt{22}}
\newcommand\huge{\@setfontsize\huge\@xxpt{25}}
\newcommand\Huge{\@setfontsize\Huge\@xxvpt{30}}
%
\DeclareOldFontCommand{\rm}{\normalfont\rmfamily}{\mathrm}
\DeclareOldFontCommand{\sf}{\normalfont\sffamily}{\mathsf}
\DeclareOldFontCommand{\tt}{\normalfont\ttfamily}{\mathtt}
\DeclareOldFontCommand{\bf}{\normalfont\bfseries}{\mathbf}
\DeclareOldFontCommand{\it}{\normalfont\itshape}{\mathit}
\DeclareOldFontCommand{\sl}{\normalfont\slshape}{\@nomath\sl}
\DeclareOldFontCommand{\sc}{\normalfont\scshape}{\@nomath\sc}
\DeclareOldFontCommand{\bi}{\bfseries\itshape}{\bfseries\itshape}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Parameters %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
\colmargin=0pc
\texttypewidthsep=0pc
%
\setlength\trimwidth{612bp}%
\setlength\trimheight{792bp}%
%
\setlength\textheight{689pt}%
%
\setlength\normaltextheight{\textheight}%
\setlength\textwidth{43.15pc}%
\setlength\columnsep{18pt}
%
\setlength\typeheight{749pt}%
\setlength\typewidth{\textwidth}%

\setlength\topmargin{30pt}%
\setlength\oddsidemargin{42pt}%
\setlength\evensidemargin{54pt}%
%
\setlength\headheight{12\p@}%
\setlength\headsep   {13pt}% \typeheight - \textheight - \headheight
\setlength\topskip   {10\p@}
%
\setlength\footskip{14.5pt}
\setlength\maxdepth{61\baselineskip}
%
\blindfoliodrop\trimheight
\advance\blindfoliodrop-\typeheight
\advance\blindfoliodrop-\topmargin
\advance\blindfoliodrop-\footskip
\advance\blindfoliodrop18pt
%
\setlength\parindent{10pt}
%
\setlength\marginparwidth {.75in}
\setlength\marginparsep{6\p@}
\setlength\marginparpush{5\p@}
%
\setlength\footnotesep{\z@}
\setlength{\skip\footins}{17\p@ \@plus 12\p@ \@minus 5\p@}% \@plus 8\p@ \@minus 8\p@}% change + based on FN font height
\skip\@mpfootins = \skip\footins
%
\setlength\floatsep    {12\p@ \@plus 2\p@ \@minus 2\p@}
\setlength\textfloatsep{6\p@ \@plus 2\p@ \@minus 4\p@}
\setlength\intextsep   {6\p@ \@plus 2\p@ \@minus 2\p@}
\setlength\dblfloatsep    {8\p@ \@plus 2\p@ \@minus 2\p@}
\setlength\dbltextfloatsep{18\p@ \@plus 2\p@ \@minus 4\p@}
\setlength\@fptop{0\p@}
\setlength\@fpsep{8\p@ \@plus 1fil}
\setlength\@fpbot{0\p@ \@plus 1fil}
\setlength\@dblfptop{0\p@}
\setlength\@dblfpsep{8\p@ \@plus 1fil}
\setlength\@dblfpbot{0\p@ \@plus 1fil}
%
\setlength\partopsep{0pt}
\setlength\lineskip{1\p@}% check if it can be flexible
\setlength\normallineskip{1\p@}%
\renewcommand\baselinestretch{}
\setlength\parskip{\z@}%{0\p@ \@plus \p@}
\@lowpenalty   51
\@medpenalty  151
\@highpenalty 301
%
\@beginparpenalty -\@lowpenalty
\@endparpenalty   -\@lowpenalty
\@itempenalty     -\@lowpenalty
%
\trimwidthval\the\trimwidth
\trimheightval\the\trimheight

\trimwidthbleedval\trimwidthval
\advance\trimwidthbleedval.25in
\trimheightbleedval\trimheightval
\advance\trimheightbleedval.25in

\def\thepaperwidth{\the\trimwidthval}
\def\thepaperheight{\the\trimheightval}

\def\thebleedpaperwidth{\the\trimwidthbleedval}
\def\thebleedpaperheight{\the\trimheightbleedval}

\AtBeginDocument{%
\iffinals
\def\papwidth{612bp}
\def\papheight{792bp}
\ifnum\pdfoutput=0
\AtBeginDvi{\special{papersize=\papwidth,\papheight}}%
\else%
  \pdfpagewidth=\papwidth  \pdfpageheight=\papheight%
\fi%
\else
\def\papwidth{\thepaperwidth}
\def\papheight{\thepaperheight}
\ifnum\pdfoutput=0
\AtBeginDvi{\special{papersize=\papwidth,\papheight}}%
\else%
  \pdfpagewidth=\papwidth  \pdfpageheight=\papheight%
\fi%
\fi
}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% End Parameters %%%%%%%%%%%%%%%%%%%%%%%%%
%

\def\journalname{Seismological Research Letters}

\def\journalshortname{Seismol. Res. Lett.}

\def\doiname{doi:}
\def\citethisarticle#1{\gdef\@citethisarticle{\fontsize{7}{9}\selectfont {\bf Cite this article as} #1}}
\citethisarticle{}
\def\citethisauthor#1{\gdef\@citethisauthor{#1}}\citethisauthor{}
\def\citethistitle#1{\gdef\@citethistitle{#1}}\citethistitle{\@title}
\def\fpage#1{\gdef\@fpage{#1}}\fpage{\thepage}
\def\vol#1{\gdef\@vol{#1}}\vol{XX}
\def\iss#1{\gdef\@iss{#1}}\iss{XX}
\def\doi#1{\gdef\@doi{#1}}\doi{}


\def\Mycitethisauthor#1{\gdef\@Mycitethisauthor{#1}}\Mycitethisauthor{}

\def\ps@myplain{%
\let\@mkboth\@gobbletwo%
\def\@evenfoot{\raisebox{-13.5pt}{\hbox to 518pt{\fontfamily{\sfdefault}\fontsize{8.5}{8.5}\selectfont{%
{\bfseries\thepage}\hspace*{8pt}{\vrule width0.5pt depth36pt height7pt}\hspace*{8pt}{\itshape\journalname}
\hfill www.srl-online.org{\fbull}Volume \@vol{\fbull}Number \@iss{\fbull}XXXX XXXX}%
}}}%
\def\@oddfoot{\raisebox{-13.5pt}{\hbox to 518pt{\fontfamily{\sfdefault}\fontsize{8.5}{8.5}\selectfont{Volume \@vol{\fbull}Number \@iss{\fbull}XXXX XXXX{\fbull}www.srl-online.org}%
\hfill{\itshape\journalname}\hspace*{8pt}{\vrule width0.5pt depth36pt height7pt}\hspace*{8pt}{\bfseries\thepage}}}}%
\def\@evenhead{}
\def\@oddhead{}
            }

\def\orcidbeforespace{\kern2pt}
\def\ausepspace{\hskip3pt}
\def\ausepcomma{,}

\def\aubetweenand{\unskip\unskip\unskip{\fontsize{11}{14}\selectfont{and}}}
\def\aulastand{\unskip\unskip\unskip{\fontsize{11}{14}\selectfont{and}}}%
\def\affcomma{,}

\def\orc#1{\href{https://orcid.org/#1}{\kern1pt\raisebox{-1.3pt}{\ifpdf\includegraphics[height=7pt]{SSA_orcid_logo_Web.pdf}\else\includegraphics[height=7pt]{SSA_orcid_logo_Web.eps}\fi}}}

\def\author[#1]#2{%
   \gdef\au@data{yes}%
   \expandafter\protected@xdef\csname author\the\authorcount\endcsname{#2\raisebox{4pt}{\fontsize{5.75}{5.75}\selectfont#1}}%
   \global\advance\authorcount1}

\def\andlastauname{\ifx\@auandbreak\undefined\else\@auandbreak\fi\gdef\AUxrefpresence{no}\unskip\unskip\aulastand{}\ausepspace\null\csname author\the\tempcount\endcsname\ifx\orcidau\YES\orcidbeforespace\null\orcid{}\fi\unskip}

\gdef\@author{%
\multiaufalse%
\global\augrouptrue%
   \global\tempauthorcount\authorcount%
   \global\advance\tempauthorcount-1%
   \global\authorcommacount\tempauthorcount%
   \global\advance\authorcommacount-1%
   \global\authortempcount0%
   \global\authortempcount0%
   \ifnum\authorcount=1%
   \gdef\AUxrefpresence{no}%
      \ignorespaces\csname author0\endcsname\orcidbeforespace\orcid{}%
   \else%
      \ifnum\authorcount=2%
      \gdef\AUxrefpresence{no}%
         \ignorespaces\csname author0\endcsname\orcidbeforespace\orcid{}\gdef\@givenname{}\ausepspace\null\aubetweenand\ausepspace\null\gdef\AUxrefpresence{no}\gdef\@ausuffix{}\csname author1\endcsname\orcidbeforespace\orcid{}%
      \else%
               \@whilenum\tempcount<\tempauthorcount%
   			       \do{%\vskip8pt
                     \gdef\AUxrefpresence{no}%
                     \ifx\orcidau\YES\gdef\orcidau{no}\else\fi%
                     \gdef\@ausuffix{}%
                     \multiautrue%
              		 \hbox{\csname author\the\tempcount\endcsname}\unskip\unskip\ifx\orcidau\YES\orcidbeforespace\orcid{}{\ausepcomma}\ausepspace\null\else{\ausepcomma}\ausepspace\null\fi\unskip\unskip%\space
                     \global\advance\tempcount1\gdef\@givenname{}}%
        \ifx\orcidau\YES\gdef\orcidau{no}\else\fi%
        \multiaufalse%
        \gdef\@ausuffix{}%
		\ausepspace\andlastauname%
               \fi%
\global\augroupfalse%
           \fi}

\def\printauthor{\@author}



\gdef\keyword@data{no}%

\def\keyword#1{\unskip%
    \gdef\keyword@data{yes}%
    \expandafter\protected@xdef\csname keyword\the\keywordcount\endcsname{#1}%
    \global\advance\keywordcount1%
}%

\def\auorcprefix{https://orcid.org/}

\newcommand{\auorc}[3][\auorcprefix]{\ %
\ifpdf%
    \includegraphics[height=7pt]{SSA_orcid_logo_Web.pdf}% [scale=.82]
\else%
    \includegraphics[height=7pt]{SSA_orcid_logo_Web.eps}% [scale=.82]
\fi%
~\href{https://orcid.org/#2}{#1#2} \MakeUppercase{#3}%
}

\def\corau#1{\gdef\@corau{\fontfamily{\sfdefault}\fontsize{7}{10}\selectfont\raggedright#1}}\corau{}

\def\cpright#1{\gdef\@cpright{#1}}\cpright{{\copyright} Seismological Society of America}

\def\affil[#1]#2#3{%
    \expandafter\protected@xdef\csname affil\the\affilcount\endcsname{#1.\kern3pt#2\ifx#3\@empty\else{\affcomma}#3\fi}%
    \global\advance\affilcount1}%

\def\semicolon{;}

\def\printaffil{%
\footnotetext{%\raggedright%
{\fontfamily{\sfdefault}\fontsize{7}{9}\selectfont\raggedright%
     \global\tempaffilcount\affilcount%
     \global\affiltempcount0%
     \global\advance\tempaffilcount-1
     \ifnum\affilcount=1%
     \ignorespaces\csname affil0\endcsname%
     \else%
     \@whilenum\affiltempcount<\tempaffilcount%
     \do{\raggedright\csname affil\the\affiltempcount\endcsname\semicolon\ %
                \global\advance\affiltempcount1}%
                \csname affil\the\affiltempcount\endcsname\fi}\par%
\ifx\@corau\@empty\else\vskip1.5pt\@corau\par\fi%
{
\vskip1.5pt\@cpright\vspace*{-9pt}\par%
}
                }%
                }%

\RequirePackage{xstring}

\def\balanceandcontinue{%
   \par
   \ifdim\pagegoal=\maxdimen
   \else
      \global\let\@outputpage\store@outputpage
      \ifdim\@colht=\pagegoal
         \global\storedboxheight\pagetotal
      \else
         \global\storedboxheight\@colht
         \global\advance\storedboxheight-\pagegoal
         \global\advance\storedboxheight\pagetotal
%         \global\advance\storedboxheight12pt
      \fi
   \fi
   \onecolumn
   \global\switchcolstrue}

\def\TwoColumn{\@ifnextchar[{\@TwoColumn}{\@TwoColumn[]}}
\def\@TwoColumn[#1]{%
   \balanceandcontinue
   \let\clearpage\relax
   \def\@tempa{#1}%
   \ifx\@tempa\@empty
      \twocolumn
   \else
      #1\global\@partialpageht\pagetotal\twocolumn
   \fi
   \let\clearpage\save@clearpage}

\def\maketitle{\par%
  \begingroup%
  \parindent0pt%
    \renewcommand\thefootnote{\@fnsymbol\c@footnote}%
    \def\@makefnmark{\rlap{\@textsuperscript{\normalfont\@thefnmark}}}%
%    \long\def\@makefntext##1{##1}%
      \long\def\@makefntext##1{\noindent
              {%
                \@textsuperscript{\normalfont\@thefnmark}}##1}%
      \if@twocolumn
        \ifnum \col@number=\@ne
          \@maketitle
        \else
            \ifmetarunon
                \TwoColumn[\@maketitle]%
            \else
                \twocolumn[\@maketitle]%
            \fi
        \fi
      \else
      \newpage
        \global\@topnum\z@   % Prevents figures from going at top of page.
        \@maketitle
      \fi
%     {\twocolumn[\@maketitle]}%
  \renewcommand\thefootnote{}%
  \ifx\printaffil\@empty%
  \else%
    \printaffil%
  \fi%
  \endgroup%
  \setcounter{footnote}{0}%
\advance\keywordcount0%
\advance\authorcount0
}

\RequirePackage[breakable]{tcolorbox}

\newtcolorbox{absbox}{breakable = true,
notitle after break,
%pad before break=0mm,
%topsep at break=-15mm,
left=9pt,right=143pt,bottom=0mm,top=0mm,colframe=color6,colback=white,boxrule=0pt,leftrule=3pt,
arc=0pt,outer arc=0pt,
pad at break=1mm}

\def\supplementhead{Supplemental Material}

\authorbelowskip=0pt

\def\mktitlespaceadjust#1{\gdef\@mktitlespaceadjust{#1pt}}\mktitlespaceadjust{0}

\def\@maketitle{\par\thispagestyle{myplain}%%
{\parindent\z@%
\vspace*{4pt}%
{%
{\titlefont\@title\vphantom{y}\@@par}}\par
\vspace*{9pt}%
{\ifnoauthor\else\authorfont%\textcolor{color2b}
\textcolor{black}%
{\@author}\vphantom{y}\fi}% \global\TitleMicroSymfalse
{\ifx\@corid\undefined\else\vskip-1.5pt{{\corresfont \unhbox\correspbox}}\fi\@par\vskip8pt}%
\ifx\@dates\undefined\else{\raggedright \historyfont \@dates\par}\fi
\vspace*{11pt}
\ifmetarunon%
    \begin{absbox}%
        {\abstractfont{\fontsize{12}{12}\selectfont{\textbf{{\abstractname}}}}}\par%
        {\@absrunon}%
    \end{absbox}%
    \begin{strip}%
    \global\setbox\ctabox\vbox\bgroup%
    \begin{flushright}%
    {\begin{minipage}[b]{123.5pt}%
    {\fontfamily{\sfdefault}\fontsize{7}{9}\selectfont\raggedright {\bfseries Cite this article as} {{\@citethisauthor} (2022). {\@citethistitle}, {{\itshape\journalshortname}} {\bfseries\@vol}, \@fpage--\@abspage@last, %\@lpage,
    {\doiname}~\href{\@doi}{\@doi}.}\vspace*{2pt}\par%
    {\fontfamily{\sfdefault}\fontseries{b}\fontsize{8.5}{8.5}\selectfont\textcolor{color1}{\supplementhead}}}%
    %\vspace*{-22pt}
    \end{minipage}}%
    \end{flushright}%
    \egroup%
    \vspace*{-\ht\ctabox}%
    \vspace*{3pc}%
    \begin{flushright}%
    {\begin{minipage}[b]{123.5pt}%
    {\fontfamily{\sfdefault}\fontsize{7}{9}\selectfont\raggedright {\bfseries Cite this article as} {{\@citethisauthor} (2022). {\@citethistitle}, {{\itshape\journalshortname}} {\bfseries\@vol}, \@fpage--\@abspage@last, %\@lpage,
    {\doiname}~\href{\@doi}{\@doi}.}\vspace*{2pt}\par%
    {\fontfamily{\sfdefault}\fontseries{b}\fontsize{8.5}{8.5}\selectfont\textcolor{color1}{\supplementhead}}}%
    %\vspace*{-22pt}
    \end{minipage}}%
    \end{flushright}%
    %\null\hspace*{-523.5pt}
    \vspace*{3pc}
    \end{strip}%
\else
    {\noindent%
    \if@twocolumn%
        \begin{tabular*}{\textwidth}{@{\hspace*{62pt}}p{383pt}@{\hspace*{-52pt}}p{123.5pt}@{}}
        {\ifx\@LDabstract\@undefined\else
        \begingroup%
        \begin{minipage}[b]{373pt}
        {\unvbox\abstractbox}
        \end{minipage}
        \endgroup%
        \vskip7.5pt\fi} & %\llap
        {\begin{minipage}[b]{123.5pt}
        {\fontfamily{\sfdefault}\fontsize{7}{9}\selectfont\raggedright {\bfseries Cite this article as} {{\@citethisauthor} (2022). {\@citethistitle}, {{\itshape\journalshortname}} {\bfseries\@vol}, \@fpage--\@abspage@last, %\@lpage,
        {\doiname}~\href{\@doi}{\@doi}.}\vspace*{2pt}\par
        {\fontfamily{\sfdefault}\fontseries{b}\fontsize{8.5}{8.5}\selectfont\textcolor{color1}{\supplementhead}}}
        %\vspace*{-22pt}
        \end{minipage}}
        \end{tabular*}
    \else%
        \begin{tabular*}{\textwidth}{@{\hspace*{-70pt}}p{.75\textwidth}@{\hspace*{70pt}}p{.25\textwidth}@{}}%
        {\ifx\@LDabstract\@undefined\else
        \begingroup%
        \begin{minipage}[b]{.75\textwidth}
        {\unvbox\abstractbox}
        \end{minipage}
        \endgroup%
        \vskip7.5pt\fi} & %\llap
        {\begin{minipage}[b]{.25\textwidth}
        {\fontfamily{\sfdefault}\fontsize{7}{9}\selectfont\raggedright {\bfseries Cite this article as} {{\@citethisauthor} (2022). {\@citethistitle}, {{\itshape\journalshortname}} {\bfseries\@vol}, \@fpage--\@abspage@last, %\@lpage,
        {\doiname}~\href{\@doi}{\@doi}.}\vspace*{2pt}\par
        {\fontfamily{\sfdefault}\fontseries{b}\fontsize{8.5}{8.5}\selectfont\textcolor{color1}{\supplementhead}}}
        %\vspace*{-22pt}
        \end{minipage}}
        \end{tabular*}
    \fi%
    }%
\fi%
\par
}
\addvspace{10pt}%
\@afterindentfalse%
\setcounter{section}{0}
\vspace*{\@mktitlespaceadjust}%
}

\newenvironment{abstract}{\global\Abstracttrue\gdef\@LDabstract{}%
\global\setbox\abstractbox\vbox%
\bgroup%
\begin{absleftbar}%
\vspace*{1pt}%
\abstractfont%
\hsize363pt%
\setlength\parindent{0pt}%
{{\fontsize{12}{12}\selectfont{\textbf{{\abstractname}}}}}\par%
   }%
   {\vspace*{-4pt}%
\end{absleftbar}%
   \egroup%
   }%

\def\absrunon#1{\gdef\@absrunon{\abstractfont#1}}

\setcounter{secnumdepth}{0}
\newcounter {part}
\newcounter {section}
\newcounter {subsection}[section]
\newcounter {subsubsection}[subsection]
\newcounter {paragraph}[subsubsection]
\newcounter {subparagraph}[paragraph]
\renewcommand\thepart          {\Roman{part}}
\renewcommand\thesection       {\arabic{section}}
\renewcommand\thesubsection    {\thesection.\arabic{subsection}}
\renewcommand\thesubsubsection {\thesubsection .\arabic{subsubsection}}
\renewcommand\theparagraph     {\thesubsubsection.\arabic{paragraph}}
\renewcommand\thesubparagraph  {\theparagraph.\arabic{subparagraph}}
%
\def\@seccntformat#1{\csname the#1\endcsname.\hspace*{4pt}}
%
\newcommand\part{%
   \if@noskipsec \leavevmode \fi
   \par
   \addvspace{4ex}%
   \@afterindentfalse
   \secdef\@part\@spart}

\def\@part[#1]#2{%
    \ifnum \c@secnumdepth >\m@ne
      \refstepcounter{part}%
      \addcontentsline{toc}{part}{\thepart\hspace{1em}#1}%
    \else
      \addcontentsline{toc}{part}{#1}%
    \fi
    {\parindent \z@ \raggedright
     \interlinepenalty \@M
     \normalfont
     \ifnum \c@secnumdepth >\m@ne
       \Large\bfseries \partname\nobreakspace\thepart
       \par\nobreak
     \fi
     \huge \bfseries #2%
     \markboth{}{}\par}%
    \nobreak
    \vskip 3ex
    \@afterheading}
\def\@spart#1{%
    {\parindent \z@ \raggedright
     \interlinepenalty \@M
     \normalfont
     \huge \bfseries #1\par}%
     \nobreak
     \vskip 3ex
     \@afterheading}

\newcommand\section{\@startsection {section}{1}{\z@}{12pt \@plus 1pt \@minus 1pt}{0.001pt}{\sectionfont\ifnarrowcol\abc{0}\else\fi}}%
\newcommand\subsection{\@startsection{subsection}{2}{\z@}{12pt \@plus 1pt \@minus 1pt}{0.001pt}{\subsectionfont\ifnarrowcol\abc{0}\else\fi}}%
\newcommand\subsubsection{\@startsection{subsubsection}{3}{\z@}{-18pt \@plus -1pt \@minus -.1pt}{0.001pt}{\subsubsectionfont\ifnarrowcol\abc{0}\else\fi}}%
\newcommand\paragraph{\@startsection{paragraph}{4}{18\p@}{-2pt \@plus -2pt \@minus -.2pt}{-1.5pt}{\paragraphfont\ifnarrowcol\abc{0}\else\fi}}%
\newcommand\subparagraph{\@startsection{subparagraph}{5}{\parindent}{-19pt \@plus -2pt \@minus -.2pt}{-1em}{\subparagraphfont\ifnarrowcol\abc{0}\else\fi}}%
\newcommand\subsubparagraph{\@startsection{subsubparagraph}{6}{\parindent}{-19pt \@plus -2pt \@minus -.2pt}{-1em}{\subsubparagraphfont\ifnarrowcol\abc{0}\else\fi}}%

\newif\ifsubsec\global\subsectrue
\newif\ifsec\global\sectrue

\def\@sect#1#2#3#4#5#6[#7]#8{\ifnum #2>\c@secnumdepth
     \let\@svsec\@empty
     \else
     \refstepcounter{#1}%
%     \refstepcounter{HL#1}%
     \let\@@protect\protect
     \def\protect{\noexpand\protect\noexpand}%
%    \global\ackhdtrue
     \edef\@svsec{%
        \@seccntformat{#1}%
     }%
     \let\protect\@@protect\fi
     \ifnum#2=2\global\subsectrue\else\fi
     \@tempskipa #5\relax
      \ifdim \@tempskipa>\z@
        \begingroup #6\relax
        \ifnum#2=1
              %\@hangfrom
	      {\noindent\hskip #3\relax{\@svsec}}% {\csname #1numfont\endcsname\@svsec}
                    {\interlinepenalty \@M {#8}\par}%
        \else
        \ifnum#2=2%\centering%
              %\@hangfrom
	      {\noindent\hskip #3\relax{\@svsec}}% {\csname #1numfont\endcsname\@svsec}
                    {\interlinepenalty \@M {#8}\par}%
         \else
         \ifnum#2=3
               \@hangfrom
           {\hskip #3\relax{\itshape\@svsec}}% {\csname #1numfont\endcsname\@svsec}
                     {\interlinepenalty \@M {\itshape #8}\par}%
%         \else
%         \ifnum#2=4
%               %\@hangfrom
%           {\hskip #3\relax{\@svsec}}% {\csname #1numfont\endcsname\@svsec}
%                     {\interlinepenalty \@M {\fontencoding{T1}\itshape #8}\par}%
        \else
              %\@hangfrom
          {\hskip #3\relax\@svsec}% {\csname #1numfont\endcsname\@svsec}
                    {\interlinepenalty \@M #8\par}%
        \fi\fi\fi%\fi
        \endgroup%
               \csname #1mark\endcsname{#7}\addcontentsline{toc}{#1}{\ifnum #2>\c@secnumdepth \else\protect\numberline{\csname the#1\endcsname}\fi#7}
        \else
        \def\@svsechd{#6\hskip #3\relax\@svsec #8\csname #1mark\endcsname{#7}}
        \fi
               \@xsect{#5}}
%
\def\@ssect#1#2#3#4#5{%
  \@tempskipa #3\relax
  \ifdim \@tempskipa>\z@
    \begingroup
      #4{%
        \@hangfrom{\hskip #1}%
          \interlinepenalty \@M {#5}\@@par}%
    \endgroup
  \else
    \def\@svsechd{#4{\hskip #1\relax #5}}%
  \fi
  \@xsect{#3}}
%
\def\@startsection#1#2#3#4#5#6{%
  \if@noskipsec \leavevmode \fi
  \par
  \@tempskipa #4\relax
  \@afterindentfalse
  \ifdim \@tempskipa <\z@
    \@tempskipa -\@tempskipa%
  \fi
  \if@nobreak
     \ifnum#2=2
     \vskip1.5pt
     \fi
     \ifnum#2=3
     \fi
    \everypar{}%
  \else
    \addpenalty\@secpenalty\addvspace\@tempskipa
  \fi
  \@ifstar
    {\@ssect{#3}{#4}{#5}{#6}}%
    {\@dblarg{\@sect{#1}{#2}{#3}{#4}{#5}{#6}}}}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  Lists %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\RequirePackage{hyperref}%
\hypersetup{
draft=false,
pdfpagemode=UseNone,
colorlinks=true,
allcolors=color4,
pdfborderstyle={/S/U/W 0},
pdfstartview={FitH 800},
breaklinks=true}

%
\topsepi6\p@ \@plus2\p@% \@minus.5\p@
\topsepii2pt% \@plus1\p@
\topsepiii2pt% \@plus1\p@
\topsepiv2pt% \@plus1\p@
\topsepv2pt% \@plus1\p@
\topsepvi2pt% \@plus1\p@
\itemsepi0pt
\itemsepii0pt
\itemsepiii0pt
\itemsepiv0pt
\itemsepv0pt
\itemsepvi0pt
%
% \def\list#1#2{%
%   \ifnum \@listdepth >10\relax
%     \@toodeep
%   \else
%     \global\advance\@listdepth\@ne
%   \fi
%   \rightmargin\z@
%   \listparindent\z@
%   \itemindent\z@
%   \csname @list\romannumeral\the\@listdepth\endcsname
%   \def\@itemlabel{#1}%
%   \let\makelabel\@mklab
%   \@nmbrlistfalse
%   #2\relax
%   \@trivlist
%   \parskip\parsep
%   \parindent\listparindent
%   \advance\linewidth -\rightmargin
%   \advance\linewidth -\leftmargin
%   \advance\@totalleftmargin \leftmargin
%   \parshape \@ne \@totalleftmargin \linewidth
%   \ignorespaces}
%
\def\@listI{\leftmargin\leftmargini
%            \labelwidth\leftmargini
%            \advance\labelwidth-\labelsep
            \parsep 0\p@%
            \topsep \topsepi
            \itemsep\itemsepi}%
\let\@listi\@listI
\@listi
\def\@listii {\leftmargin\leftmarginii
              \labelwidth\leftmarginii
              \advance\labelwidth-\labelsep
              \topsep\topsepii
              \parsep 0pt
              \itemsep\itemsepii}
\def\@listiii {\leftmargin\leftmarginiii
              \labelwidth\leftmarginiii
              \advance\labelwidth-\labelsep
              \topsep\topsepiii
              \parsep 0pt
              \itemsep\itemsepiii}
\def\@listiv {\leftmargin\leftmarginiv
              \labelwidth\leftmarginiv
              \advance\labelwidth-\labelsep}
\def\@listv  {\leftmargin\leftmarginv
              \labelwidth\leftmarginv
              \advance\labelwidth-\labelsep}
\def\@listvi {\leftmargin\leftmarginvi
              \labelwidth\leftmarginvi
              \advance\labelwidth-\labelsep}

%
% \setlength\leftmargini  {2.5em}
% \setlength\leftmarginii  {2.2em}
% \setlength\leftmarginiii {1.87em}
% \setlength\leftmarginiv  {1.7em}
% \setlength\leftmarginv  {1em}
% \setlength\leftmarginvi {1em}
% \setlength\leftmarginvii {1em}
% \setlength\leftmarginviii {1em}
% \setlength\leftmarginix {1em}
% \setlength\leftmarginx {1em}
% \setlength\leftmargin    {\leftmargini}
% %
% \setlength  \labelsep  {\LabelSep}
% \setlength  \labelwidth{\leftmargini}
% \addtolength\labelwidth{-\labelsep}
%
% \newcommand\theenumv{\Alph{enumv}}
% \newcommand\theenumvi{\Alph{enumvi}}
% \newcommand\theenumvii{\roman{enumvii}}
% \newcommand\theenumviii{\Alph{enumviii}}
% \newcommand\theenumix{\Alph{enumix}}
% \newcommand\theenumx{\roman{enumx}}
% \renewcommand\theenumi{\arabic{enumi}}
% \renewcommand\theenumii{\alph{enumii}}
% \renewcommand\theenumiii{\roman{enumiii}}
% \renewcommand\theenumiv{\Alph{enumiv}}
% \renewcommand\theenumv{\Alph{enumv}}
% \renewcommand\theenumvi{\Alph{enumvi}}
% \renewcommand\theenumvii{\Alph{enumvii}}
% \renewcommand\theenumviii{\Alph{enumviii}}
% \renewcommand\theenumix{\Alph{enumix}}
% \renewcommand\theenumx{\Alph{enumx}}
% \newcommand\labelenumi{\theenumi.}
% \newcommand\labelenumii{\theenumii.}
% \newcommand\labelenumiii{\theenumiii.}
% \newcommand\labelenumiv{\theenumiv.}
% \newcommand\labelenumv{\theenumv.}
% \newcommand\labelenumvi{\theenumvi.}
% \newcommand\labelenumvii{\theenumvii.}
% \newcommand\labelenumviii{\theenumviii.}
% \newcommand\labelenumix{\theenumix.}
% \newcommand\labelenumx{\theenumx.}
% \renewcommand\p@enumii{\theenumi}
% \renewcommand\p@enumiii{\theenumi(\theenumii)}
% \renewcommand\p@enumiv{\p@enumiii\theenumiii}
% \font\lcir = lcircle10 at 10pt
% \newcommand\bulls{\raise1.5pt\hbox{\lcir\char'162}}
% \def\textbullet{\leavevmode\raise2pt\hbox{\hskip2pt\bulls}}
% \def\textendash{{\bf--}}
% \def\textasteriskcentered{\leavevmode\raise-1.5pt\hbox{*}}
% \def\textperiodcentered{\leavevmode\raise1.5pt\hbox{\bulls}}
% \newcommand\labelitemi{\textbullet}
% \newcommand\labelitemii{\normalfont\bfseries \textendash}
% \newcommand\labelitemiii{\textasteriskcentered}
% \newcommand\labelitemiv{\textperiodcentered}
% \newcommand\labelitemv{\textperiodcentered}
% \newcommand\labelitemvi{\textbullet}
% \newcommand\labelitemvii{\normalfont\bfseries \textendash}
% \newcommand\labelitemviii{\textasteriskcentered}
% \newcommand\labelitemix{\textperiodcentered}
% \newcommand\labelitemx{\textperiodcentered}
%
\if@twocolumn
  \setlength\leftmargini  {1.25em}
\else
  \setlength\leftmargini  {1.75em}
\fi
\leftmargin  \leftmargini
\setlength\leftmarginii  {1.25em}
\setlength\leftmarginiii {1.12em}
\setlength\leftmarginiv  {0.95em}
\if@twocolumn
  \setlength\leftmarginv  {.5em}
  \setlength\leftmarginvi {.5em}
\else
  \setlength\leftmarginv  {1em}
  \setlength\leftmarginvi {1em}
\fi
\setlength  \labelsep  {.5em}
\setlength  \labelwidth{\leftmargini}
\addtolength\labelwidth{-\labelsep}
\@beginparpenalty -\@lowpenalty
\@endparpenalty   -\@lowpenalty
\@itempenalty     -\@lowpenalty
\renewcommand\theenumi{\@arabic\c@enumi}
\renewcommand\theenumii{\@alph\c@enumii}
\renewcommand\theenumiii{\@roman\c@enumiii}
\renewcommand\theenumiv{\@Alph\c@enumiv}
\newcommand\labelenumi{\theenumi.}
\newcommand\labelenumii{(\theenumii)}
\newcommand\labelenumiii{\theenumiii.}
\newcommand\labelenumiv{\theenumiv.}
\renewcommand\p@enumii{\theenumi}
\renewcommand\p@enumiii{\theenumi(\theenumii)}
\renewcommand\p@enumiv{\p@enumiii\theenumiii}
\newcommand\labelitemi  {\labelitemfont \textbullet}
\newcommand\labelitemii {\labelitemfont \textbullet}
\newcommand\labelitemiii{\labelitemfont \textasteriskcentered}
\newcommand\labelitemiv {\labelitemfont \textperiodcentered}
\newcommand\labelitemfont{\normalfont}
\newenvironment{description}
               {\list{}{\labelwidth\z@ \itemindent-\leftmargin
                        \let\makelabel\descriptionlabel}}
               {\endlist}
\newcommand*\descriptionlabel[1]{\hspace\labelsep
                                \normalfont\bfseries #1}
%
\newenvironment{verse}
               {\let\\\@centercr
                \list{}{\itemsep      \z@
                        \itemindent   -1.5em%
                        \listparindent\itemindent
                        \rightmargin  \leftmargin
                        \advance\leftmargin 1.5em}%
                \item\relax}
               {\endlist}
\newenvironment{quotation}
               {\list{}{\listparindent 1.5em%
                        \itemindent    \listparindent
                        \rightmargin   \leftmargin
                        \parsep        \z@ \@plus\p@}%
                \item\relax}
               {\endlist}
\newenvironment{quote}
               {\list{}{\rightmargin\leftmargin}%
                \item\relax}
               {\endlist}

\newcommand\appendix{\par
  \setcounter{section}{0}%
  \setcounter{subsection}{0}%
  \gdef\thesection{\@Alph\c@section}}

%\setlength\arraycolsep{5\p@}
%\setlength\tabcolsep{6\p@}
%\setlength\arrayrulewidth{.4\p@}
%\setlength\doublerulesep{2\p@}
%\setlength\tabbingsep{\labelsep}
\skip\@mpfootins = \skip\footins
%\setlength\fboxsep{3\p@}
%\setlength\fboxrule{.4\p@}
%\renewcommand \theequation {\@arabic\c@equation}
\newcounter{figure}


\setcounter{topnumber}{5}
\renewcommand\topfraction{.9}
\setcounter{bottomnumber}{5}
\renewcommand\bottomfraction{.9}
\setcounter{totalnumber}{5}
\renewcommand\textfraction{.09}
\renewcommand\floatpagefraction{.901}
\setcounter{dbltopnumber}{2}
\renewcommand\dbltopfraction{.9}
\renewcommand\dblfloatpagefraction{.901}
%
\setlength\abovecaptionskip{13\p@}
\setlength\belowcaptionskip{\z@}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  Figures %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
\def\figcaptiondesc#1{\gdef\@figcaptiondesc{#1}}
\figcaptiondesc{}
%
\renewcommand \thefigure {\@arabic\c@figure}
\def\fps@figure{tbp}
\def\ftype@figure{1}
\def\ext@figure{lof}

\def\fnum@figure{\figurename\nobreakspace\thefigure}
\newenvironment{figure}
               {\@float{figure}}
               {\end@float}
\newenvironment{figure*}
               {\global\twocolcaptiontrue\@dblfloat{figure}}
               {\end@dblfloat\global\twocolcaptionfalse\global\rightcaptionfalse}


\newif\ifwidercaption\global\widercaptionfalse
\newif\ifnofigrule\global\nofigrulefalse

\def\@makefigurecaption#1#2{%\figcaptionfont%
\global\setbox\figcapbox\vbox{\noindent{\figcaptionfont{\figcaptionnumfont{#1.}\hskip\fignumcapsep}{\figcaptiondescfont\@figcaptiondesc}\hskip\figcapdescsep#2\par}}%
\iftwocolcaption%
\ifdim\ht\figcapbox<17pt%
\hbox to \textwidth{\hfill\figcaptionfont{\figcaptionnumfont{#1.}\hskip\fignumcapsep}{\figcaptiondescfont\@figcaptiondesc}\hskip\figcapdescsep#2\hfill}%
\else%
\vspace*{-1.18\baselineskip}%
\begin{multicols}{2}%
\noindent{\figcaptionfont{\figcaptionnumfont{#1.}\hskip\fignumcapsep}{\figcaptiondescfont\@figcaptiondesc}\hskip\figcapdescsep#2\par}% \textcolor{color3}
\end{multicols}%
\vspace*{-12pt}%
{\vrule height 0.25pt width \ifnofigrule0pt\else\hsize\fi depth0pt}%
\fi%
\else%
\noindent%
\ifwidercaption\hsize375pt\else\fi%
{\figcaptionfont{\figcaptionnumfont{#1.}\hskip\fignumcapsep}{\figcaptiondescfont\@figcaptiondesc}\hskip\figcapdescsep#2\par\vspace*{-1.5pt}}% \textcolor{color3}
{\vrule height 0.25pt width \ifnofigrule0pt\else\ifwidercaption375pt\else\hsize\fi\fi depth0pt}%
\fi%
\global\widercaptionfalse%
\global\nofigrulefalse%
}

\newcounter{Figcap}
\def\Figcapitem#1{\refstepcounter{Figcap}\figcaptionfont{\figcaptionnumfont{\figurename\nobreakspace\theFigcap.}\hskip\fignumcapsep}{\figcaptiondescfont\@figcaptiondesc}\hskip\figcapdescsep#1\par\vskip6pt}

\RequirePackage{subcaption}

\def\FigName{figure}
%
\long\def\@makecaption#1#2{%
    \ifx\FigName\@captype
      \vskip\abovecaptionskip
      \@makefigurecaption{#1}{#2}%
      \vskip\belowcaptionskip
      %\vskip0pt
    \else
      \@maketablecaption{#1}{#2}%
      \vskip\belowcaptionskip
    \fi}
%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% B-Booktabs Materials %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\@lastruleclass=0
\def\futurenonspacelet#1{\def\@BTcs{#1}%
   \afterassignment\@BTfnslone\let\nexttoken= }
\def\@BTfnslone{\expandafter\futurelet\@BTcs\@BTfnsltwo}
\def\@BTfnsltwo{\expandafter\ifx\@BTcs\@sptoken\let\next=\@BTfnslthree
   \else\let\next=\nexttoken\fi \next}
\def\@BTfnslthree{\afterassignment\@BTfnslone\let\next= }
\def\topline{\noalign{\ifnum0=`}\fi
  \@aboverulesep=\aboverulesep
  \global\@belowrulesep=\belowrulesep %global cos for use in the next noalign
  \global\@thisruleclass=\@ne
  \@ifnextchar[{\@BTrule}{\@BTrule[\toprulewidth]}}
\def\midline{\noalign{\ifnum0=`}\fi
  \@aboverulesep=\aboverulesep
  \global\@belowrulesep=\belowrulesep
  \global\@thisruleclass=\@ne
  \@ifnextchar[{\@BTrule}{\@BTrule[\midrulewidth]}}
\def\botline{\noalign{\ifnum0=`}\fi
  \@aboverulesep=\aboverulesep
  \global\@belowrulesep=\belowrulesep
  \global\@thisruleclass=\@ne
  \@ifnextchar[{\@BTrule}{\@BTrule[\botrulewidth]}}
\def\specialrule#1#2#3{\noalign{\ifnum0=`}\fi
  \@aboverulesep=#2\global\@belowrulesep=#3\global\@thisruleclass=\tw@
  \@BTrule[#1]}
\def\addlinespace{\noalign{\ifnum0=`}\fi
  \@ifnextchar[{\@addspace}{\@addspace[\defaultaddspace]}}
\def\@addspace[#1]{\global\@belowrulesep=#1\global\@thisruleclass=\tw@
  \futurelet\@tempa\@BTendrule}
\def\@BTrule[#1]{%
  \global\@thisrulewidth=#1\relax
  \ifnum\@thisruleclass=\tw@\vskip\@aboverulesep\else
  \ifnum\@lastruleclass=\z@\vskip\@aboverulesep\else
  \ifnum\@lastruleclass=\@ne\vskip\doublerulesep\fi\fi\fi
  \ifx\longtable\undefined
     \let\@BTswitch\@BTnormal
  \else\ifx\hline\LT@hline
     \let\@BTswitch\@BLTrule
  \else
     \let\@BTswitch\@BTnormal
  \fi\fi
  \@BTswitch}
\AtBeginDocument{%
   \providecommand*\CT@arc@{}}%% colortbl support
\def\@BTnormal{{%
    {\CT@arc@\hrule\@height\@thisrulewidth}%
    \futurenonspacelet\@tempa\@BTendrule}}
\def\@BLTrule{\@ifnextchar({\@@BLTrule}{\@@BLTrule()}}
\def\@@BLTrule(#1){\@setrulekerning{#1}%
\global\@cmidlb\LT@cols
\ifnum0=`{\fi}%
\@cmidruleb
\noalign{\ifnum0=`}\fi
\futurenonspacelet\@tempa\@BTendrule}
\def\@BTendrule{%
\ifx\@tempa\toprule\global\@lastruleclass=\@thisruleclass
\else
    \ifx\@tempa\midrule\global\@lastruleclass=\@thisruleclass
    \else
        \ifx\@tempa\bottomrule\global\@lastruleclass=\@thisruleclass
        \else
            \ifx\@tempa\cmidrule\global\@lastruleclass=\@thisruleclass
            \else
                \ifx\@tempa\specialrule\global\@lastruleclass=\@thisruleclass
                \else
                    \ifx\@tempa\addlinespace\global\@lastruleclass=\@thisruleclass
                    \else
                        \global\@lastruleclass=\z@
                    \fi
                \fi
            \fi
        \fi
    \fi
\fi
  \ifnum\@lastruleclass=\@ne\relax\else\vskip\@belowrulesep\fi
  \ifnum0=`{\fi}
  }
\def\@setrulekerning#1{%
   \global\let\cmrkern@l\z@
   \global\let\cmrkern@r\z@
   \@tfor\@tempa :=#1\do
  {\def\@tempb{r}%
   \ifx\@tempa\@tempb
      \global\let\cmrkern@r\cmidrulekern
      \def\cmrsideswitch{\cmrkern@r}%
   \else
      \def\@tempb{l}%
      \ifx\@tempa\@tempb
         \global\let\cmrkern@l\cmidrulekern
         \def\cmrsideswitch{\cmrkern@l}%
      \else
         \global\expandafter\let\cmrsideswitch\@tempa
      \fi
   \fi}}
\def\cmidrule{\noalign{\ifnum0=`}\fi
    \@ifnextchar[{\@cmidrule}{\@cmidrule[\cmidrulewidth]}}
\def\@cmidrule[#1]{\@ifnextchar({\@@cmidrule[#1]}{\@@cmidrule[#1]()}}
\def\@@cmidrule[#1](#2)#3{\@@@cmidrule[#3]{#1}{#2}}
\def\@@@cmidrule[#1-#2]#3#4{\global\@cmidla#1\relax
    \global\advance\@cmidla\m@ne
    \ifnum\@cmidla>0\global\let\@gtempa\@cmidrulea\else
    \global\let\@gtempa\@cmidruleb\fi
    \global\@cmidlb#2\relax
    \global\advance\@cmidlb-\@cmidla
    \global\@thisrulewidth=#3
    \@setrulekerning{#4}
    \ifnum\@lastruleclass=\z@\vskip \aboverulesep\fi
    \ifnum0=`{\fi}\@gtempa
    \noalign{\ifnum0=`}\fi\futurenonspacelet\@tempa\@xcmidrule}
\def\@xcmidrule{%
   \ifx\@tempa\cmidrule
       \vskip-\@thisrulewidth
       \global\@lastruleclass=\@ne
   \else \ifx\@tempa\morecmidrules
       \vskip \cmidrulesep
       \global\@lastruleclass=\@ne\else
       \vskip \belowrulesep
       \global\@lastruleclass=\z@
   \fi\fi
   \ifnum0=`{\fi}}
\def\@cmidrulea{%
   \multispan\@cmidla&\multispan\@cmidlb
   \unskip\hskip\cmrkern@l%
  {\CT@arc@\leaders\hrule \@height\@thisrulewidth\hfill}%
   \hskip\cmrkern@r\cr}%
\def\@cmidruleb{%
    \multispan\@cmidlb
    \unskip\hskip \cmrkern@l%
   {\CT@arc@\leaders\hrule \@height\@thisrulewidth\hfill}%
    \hskip\cmrkern@r\cr}%
\def\morecmidrules{\noalign{\relax}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% E-Booktabs Materials %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newcounter{table}
\renewcommand\thetable{\@arabic\c@table}
\def\fps@table{tbp}
\def\ftype@table{2}
\def\ext@table{lot}
\def\fnum@table{\tablename\nobreakspace\thetable.\enspace}
%
\def\tablefootnote#1{
        \global\advance\tfootcount1\expandafter\gdef\csname tablefootnote\the\tfootcount\endcsname{#1}}
%
\newenvironment{table}
               {\let\footnote\tablefootnote\@float{table}}
               {%
                  \ifx\@tablebelowspace\@empty%
                  \else%
                    \vspace*{\@tablebelowspace}\tablebelowspace{}%
                  \fi%
		\end@float}
%
\def\tablebelowspace#1{\gdef\@tablebelowspace{#1}}\tablebelowspace{}
%
\newenvironment{table*}
               {\let\footnote\tablefootnote\global\twocoltabletrue\@dblfloat{table}}
               {%
                  \ifx\@tablebelowspace\@empty%
                  \else%
                    \vspace*{\@tablebelowspace}\tablebelowspace{}%
                  \fi%
               \end@dblfloat\global\twocoltablefalse}
%
\def\@cline#1-#2\@nil{%
  \omit
  \@multicnt#1%
  \advance\@multispan\m@ne
  \ifnum\@multicnt=\@ne\@firstofone{&\omit}\fi
  \@multicnt#2%
  \advance\@multicnt-#1%
  \advance\@multispan\@ne
  \leaders\hrule\@height\arrayrulewidth\hfill
  \cr
  \noalign{\vskip-\arrayrulewidth}}
%
\def\@testtabularcr{}
%%%%%%%%%%%%%%% B-Table Rules %%%%%%%%%%%%%%
%
\toprulewidth=0.47pt%.42pt
\midrulewidth=0.47pt%.42pt
\botrulewidth=\toprulewidth
\cmidrulewidth=0.25pt%.42pt
\belowbottomsep=0pt
\abovetopsep=0pt
\cmidrulesep=\doublerulesep
\cmidrulekern=.5em
\defaultaddspace=.5em
%
%%%%%%%%%%%%%%% E-Table Rules %%%%%%%%%%%%%%

\def\Hline{%
  \noalign{\ifnum0=`}\fi\hrule \@height .5pt \futurelet%\@height \arrayrulewidth
   \@tempa\@xhline}
%
\def\Mline{%
  \noalign{\ifnum0=`}\fi\hrule \@height 0pt \futurelet%\@height \arrayrulewidth
   \@tempa\@xhline}
%
\def\@maketablecaption#1#2{%
\hsize\tablewidth%
                        {\tablecaptionfont\ifunnumtable\else\noindent{\tablecaptionnumfont\fnum@table}\fi\newline%\hskip\tablenumcapsep%
                        {\bfseries\mathversion{bold}#2}\vphantom{y}\par}%\global\unnumtablefalse%
}

\def\trowsep{\\[5.5pt]}

\def\tbl#1#2{%
\global\setbox\tempbox\hbox{\tablefont#2}%
\global\setbox\temptbox\hbox{\tablecaptionfont#1}%
\global\setbox\temptbox\hbox{{\tablecaptionfont\ifunnumtable\else\noindent{\tablecaptionnumfont\fnum@table\hskip\tablenumcapsep}\fi#1\vphantom{y}\par}}%
\global\tablewidth\wd\tempbox%
\def\tabstartrule{\vspace*{3.5pt}{{\vrule width\tablewidth height3pt depth0pt}}}
%%%
\ifsidewaystable%
     \tabledim\textheight%
     \advance\tabledim-\wd\tempbox%
     \ifdim\tabledim>0pt%
        \divide\tabledim2%
     \else%
        \global\tabledim0pt%
     \fi%
     \global\tableleftskip0pt%\tabledim% plus1fill%
     \global\tablerightskip0pt%\tabledim plus1fill%
     \global\tabnoteleftskip\tabledim%
     \global\tabnoterightskip\tabledim%
     %\caption{#1}%
     \ifdim\wd\temptbox > \hsize
        \caption{#1}%
     \else
        \caption{#1}
     \fi
     {\centerline{\box\tempbox}}%
\else%
  \ifdim\wd\tempbox > 42pc%
    \ifodd\c@page%
      \tabledim\typewidth%
      \advance\tabledim-\wd\tempbox%
      \ifdim\tabledim>0pt%
                \divide\tabledim2%
      \else%
                \global\tabledim0pt%
      \fi%
      \tableleftskip\tabledim% plus1fill%
      \tablerightskip-9pc%
      \advance\tablerightskip\tabledim plus1fill%
      \tabnoteleftskip\tabledim%
      \tabnoterightskip-9pc%
      \advance\tabnoterightskip\tabledim%
      \caption{#1}%
      {\centerline{\box\tempbox}}%
    \else%
      \tabledim\typewidth%
      \advance\tabledim-\wd\tempbox%
      \ifdim\tabledim>0pt%
                    \divide\tabledim2%
      \else%
                \global\tabledim0pt%
      \fi%
      \tableleftskip-9pc% plus1fill%
      \advance\tableleftskip\tabledim%
      \tablerightskip\tabledim%
      \tabnoteleftskip-9pc%
      \advance\tabnoteleftskip\tabledim%
      \tabnoterightskip\tabledim%
      \caption{#1}%
      {\centerline{\box\tempbox}}%
    \fi%
  \else%
       \tabledim\hsize%
       \advance\tabledim-\wd\tempbox%
       \ifdim\tabledim>0pt%
                    \divide\tabledim2%
       \else%
                \global\tabledim0pt%
       \fi%
       \global\tableleftskip0pt%\tabledim% plus1fill%
       \global\tablerightskip0pt%\tabledim%
       \global\tabnoteleftskip\tabledim%
       %\advance\tabnoteleftskip by -1pt%
       \global\tabnoterightskip\tabledim%
%       \ifdim\wd\temptbox > \hsize
\centering%
%        \noindent\tabstartrule\par%
        \hbox to \iftwocoltable\textwidth\else\columnwidth\fi{\hfill\vbox{\caption{#1}}\hfill}%
%       \else
%        \caption{#1}
%       \fi%
       {\par\centerline{\box\tempbox}}%
%     \fi%
%     \fi%
  \fi%
\fi%
}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  Math %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
\setlength\arraycolsep{1.5\p@}
\setlength\tabcolsep{6\p@}
\setlength\arrayrulewidth{.25\p@}
\setlength\doublerulesep{2\p@}
\setlength\tabbingsep{\labelsep}
\setlength\fboxsep{3\p@}
\setlength\fboxrule{.4\p@}
%
\setlength\columnsep{18\p@}
\setlength\columnseprule{0\p@}
%
\renewcommand\theequation{\arabic{equation}}
%
\def\bstrut{\vrule width0pt depth6pt}
\def\tstrut{\vrule width0pt height9pt}
%
% \def\text#1{\mathchoice
% {\hbox{\fontsize{\tf@size}{\tf@size}\selectfont#1}}%
% {\hbox{\fontsize{\tf@size}{\tf@size}\selectfont#1}}%
% {\hbox{\fontsize{\sf@size}{\sf@size}\selectfont#1}}%
% {\hbox{\fontsize{\ssf@size}{\ssf@size}\selectfont#1}}}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  Footnote %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\renewcommand\footnoterule{%
  \kern-3\p@
  {\hrule\@width\hsize height0.5pt}
  \vspace*{1.5pt}
  \kern2.6\p@}

\@addtoreset{footnote}{chapter}

\foot@parindent0pt

% \newcommand\@makefntext[1]{%
%     \ifpddata%
%     \else%
%     \ifFoottext%
%     \else%
%     \fi\fi%
%     \noindent%
%     \hb@xt@\foot@parindent{\hss\@makefnmark}#1}

\newcommand\@makefntext[1]{%
%    \parindent 1em%
    \noindent
    \hb@xt@.4em{\hss\@makefnmark}#1}


\def\@makefnmark{%
\ifpddata%
\setcounter{footnote}{0}%
\else%
\hbox{\@textsuperscript{\normalfont\@thefnmark}}%
\fi}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  End Footnote %%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  Page styles  %%%%%%%%%%%%%%%%%%%%%%%%%%%%
\def\oddfolio{{\thepage}}
\def\evenfolio{{\thepage}}
%
\def\versorh#1{\gdef\@versorh{#1}}\versorh{}
\def\rectorh#1{\gdef\@rectorh{#1}}\rectorh{}
\def\fbull{\hspace*{7pt}{{\fontsize{18}{18}\selectfont\bf \raisebox{-2pt}{$\cdot$}}}\hspace*{7pt}}
%
\def\ps@headings{%
    \def\@oddfoot{\raisebox{-13.5pt}{\hbox to 518pt{\fontfamily{\sfdefault}\fontsize{8.5}{8.5}\selectfont{Volume \@vol{\fbull}Number \@iss{\fbull}XXXX XXXX{\fbull}www.srl-online.org}%
\hfill{\itshape\journalname}\hspace*{8pt}{\vrule width0.5pt depth36pt height7pt}\hspace*{8pt}{\bfseries\thepage}}}}
    \def\@evenfoot{\raisebox{-13.5pt}{\hbox to 518pt{\fontfamily{\sfdefault}\fontsize{8.5}{8.5}\selectfont{%
{\bfseries\thepage}\hspace*{8pt}{\vrule width0.5pt depth36pt height7pt}\hspace*{8pt}{\itshape\journalname}
\hfill www.srl-online.org{\fbull}Volume \@vol{\fbull}Number \@iss{\fbull}XXXX XXXX}%
}}}%
    \let\@oddhead\@empty%
    \let\@evenhead\@empty%
    \let\@mkboth\markboth
    \def\chaptermark##1{\markboth{\@versorh}{\@rectorh}}%
%    \def\sectionmark##1{\markright{##1}}%
    }%

\DeclareOldFontCommand{\rm}{\normalfont\rmfamily}{\mathrm}
\DeclareOldFontCommand{\sf}{\normalfont\sffamily}{\mathsf}
\DeclareOldFontCommand{\tt}{\normalfont\ttfamily}{\mathtt}
\DeclareOldFontCommand{\bf}{\normalfont\bfseries}{\mathbf}
\DeclareOldFontCommand{\it}{\normalfont\itshape}{\mathit}
\DeclareOldFontCommand{\sl}{\normalfont\slshape}{\@nomath\sl}
\DeclareOldFontCommand{\sc}{\normalfont\scshape}{\@nomath\sc}
\DeclareRobustCommand*\cal{\@fontswitch\relax\mathcal}
\DeclareRobustCommand*\mit{\@fontswitch\relax\mathnormal}
\newcommand\@pnumwidth{1.55em}
\newcommand\@tocrmarg{2.55em}
\newcommand\@dotsep{4.5}
\setcounter{tocdepth}{3}
\newcommand\tableofcontents{%
    \section*{\contentsname
        \@mkboth{%
           \MakeUppercase\contentsname}{\MakeUppercase\contentsname}}%
    \@starttoc{toc}%
    }
\newcommand*\l@part[2]{%
  \ifnum \c@tocdepth >-2\relax
    \addpenalty\@secpenalty
    \addvspace{2.25em \@plus\p@}%
    \setlength\@tempdima{3em}%
    \begingroup
      \parindent \z@ \rightskip \@pnumwidth
      \parfillskip -\@pnumwidth
      {\leavevmode
       \large \bfseries #1\hfil
       \hb@xt@\@pnumwidth{\hss #2%
                          \kern-\p@\kern\p@}}\par
       \nobreak
    \endgroup
  \fi}
\newcommand*\l@section[2]{%
  \ifnum \c@tocdepth >\z@
    \addpenalty\@secpenalty
    \addvspace{1.0em \@plus\p@}%
    \setlength\@tempdima{1.5em}%
    \begingroup
      \parindent \z@ \rightskip \@pnumwidth
      \parfillskip -\@pnumwidth
      \leavevmode \bfseries
      \advance\leftskip\@tempdima
      \hskip -\leftskip
      #1\nobreak\hfil
      \nobreak\hb@xt@\@pnumwidth{\hss #2%
                                 \kern-\p@\kern\p@}\par
    \endgroup
  \fi}
\newcommand*\l@subsection{\@dottedtocline{2}{1.5em}{2.3em}}
\newcommand*\l@subsubsection{\@dottedtocline{3}{3.8em}{3.2em}}
\newcommand*\l@paragraph{\@dottedtocline{4}{7.0em}{4.1em}}
\newcommand*\l@subparagraph{\@dottedtocline{5}{10em}{5em}}
\newcommand\listoffigures{%
    \section*{\listfigurename}%
      \@mkboth{\MakeUppercase\listfigurename}%
              {\MakeUppercase\listfigurename}%
    \@starttoc{lof}%
    }
\newcommand*\l@figure{\@dottedtocline{1}{1.5em}{2.3em}}
\newcommand\listoftables{%
    \section*{\listtablename}%
      \@mkboth{%
          \MakeUppercase\listtablename}%
         {\MakeUppercase\listtablename}%
    \@starttoc{lot}%
    }
\let\l@table\l@figure

\newenvironment{datres}[1][\dataresname]%
{\bgroup\section{#1}\acknowfont}%
{\par\egroup}

\newenvironment{ack}[1][\ackheadname]%
{\bgroup\section{#1}\acknowfont}%
{\par\egroup}


%\newdimen\bibindent
\setlength\bibindent{1.5em}
\newenvironment{thebibliography}[1]
     {\section*{\refname}%
      \@mkboth{\MakeUppercase\refname}{\MakeUppercase\refname}%
      \list{\@biblabel{\@arabic\c@enumiv}}%
           {\settowidth\labelwidth{\@biblabel{#1}}%
            \leftmargin\labelwidth
            \advance\leftmargin\labelsep
            \@openbib@code
            \usecounter{enumiv}%
            \let\p@enumiv\@empty
            \renewcommand\theenumiv{\@arabic\c@enumiv}}%
      \sloppy
      \clubpenalty4000
      \@clubpenalty \clubpenalty
      \widowpenalty4000%
      \sfcode`\.\@m}
     {\def\@noitemerr
       {\@latex@warning{Empty `thebibliography' environment}}%
      \endlist}

\RequirePackage[authoryear]{natbib}

\def\bibfont{\fontsize{9}{11.91}\selectfont}
\setlength{\bibhang}{12pt}
\def\refdoi#1{\href{http://dx.doi.org/#1}{#1}}


\renewenvironment{thebibliography}[1]{%
 \bibsection
 \parindent\z@
 \bibpreamble
 \bibfont
 \list{\@biblabel{\the\c@NAT@ctr}}{\@bibsetup{#1}\global\c@NAT@ctr\z@}%
 \ifNAT@openbib
   \renewcommand\newblock{\par}%
 \else
   \renewcommand\newblock{\hskip .11em \@plus.33em \@minus.07em}%
 \fi
 \sloppy\clubpenalty10000\widowpenalty10000
 \sfcode`\.\@m
 \let\NAT@bibitem@first@sw\@firstoftwo
    \let\citeN\cite \let\shortcite\cite
    \let\citeasnoun\cite
}{%
 \bibitem@fin
 \bibpostamble
 \def\@noitemerr{%
  \PackageWarning{natbib}{Empty `thebibliography' environment}%
 }%
 \endlist
 \bibcleanup
}%

\newcommand\newblock{}
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  End Bibliography Section %%%%%%%%%%%%%%%%


%\newcommand\newblock{\hskip .11em\@plus.33em\@minus.07em}
\let\@openbib@code\@empty
\newenvironment{theindex}
               {\if@twocolumn
                  \@restonecolfalse
                \else
                  \@restonecoltrue
                \fi
                \twocolumn[\section*{\indexname}]%
                \@mkboth{\MakeUppercase\indexname}%
                        {\MakeUppercase\indexname}%
                \thispagestyle{plain}\parindent\z@
                \parskip\z@ \@plus .3\p@\relax
                \columnseprule \z@
                \columnsep 35\p@
                \let\item\@idxitem}
               {\if@restonecol\onecolumn\else\clearpage\fi}
\newcommand\@idxitem{\par\hangindent 40\p@}
\newcommand\subitem{\@idxitem \hspace*{20\p@}}
\newcommand\subsubitem{\@idxitem \hspace*{30\p@}}
\newcommand\indexspace{\par \vskip 10\p@ \@plus5\p@ \@minus3\p@\relax}

\newcommand\contentsname{Contents}
\newcommand\listfigurename{List of Figures}
\newcommand\listtablename{List of Tables}
\renewcommand\refname{References}
\newcommand\indexname{Index}
\newcommand\figurename{Figure}
\newcommand\tablename{\uppercase{Table}}
\newcommand\partname{Part}
\newcommand\appendixname{Appendix}
\newcommand\abstractname{Abstract}
\newcommand\ackheadname{Acknowledgments}
\newcommand\dataresname{Data and Resources}

\def\today{\ifcase\month\or
  January\or February\or March\or April\or May\or June\or
  July\or August\or September\or October\or November\or December\fi
  \space\number\day, \number\year}

%
\global\letterwidth210mm%
\global\letterheight297mm%
%
\global\legalwidth8.5in%
\global\legalheight14in%
%
\global\tabloidwidth11in%
\global\tabloidheight17in%
%
\iflettersize%%%%%%%
  \voffset\letterheight%
  \advance\voffset-\trimheight%
  \divide\voffset2%
  %
  \hoffset\letterwidth%
  \advance\hoffset-\trimwidth%
  \divide\hoffset2%
  %
\else%
  \iflegalsize%
     \voffset\legalheight%
     \advance\voffset-\trimheight%
     \divide\voffset2%
     %
     \hoffset\legalwidth%
     \advance\hoffset-\trimwidth%
     \divide\hoffset2%
     %
  \else%
     \iftabloidsize%
        \voffset\tabloidheight%
        \advance\voffset-\trimheight%
        \divide\voffset2%
        %
        \hoffset\tabloidwidth%
        \advance\hoffset-\trimwidth%
        \divide\hoffset2%
        %
     \fi%
  \fi%
\fi%
%
\AtBeginDocument{%
\advance\voffset-101.5pt%
\advance\hoffset-63.5pt%
}


%%%%%%%%%%%%%%%%%%%%%%%%% For table column alignments thorughout the journals %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\hbadness=10000
\vbadness=10000
%\brokenpenalty=10000
\doublehyphendemerits 1000000
\finalhyphendemerits=1000000
\predisplaypenalty=0
%\clubpenalty=10000
%\widowpenalty=10000
\hyphenpenalty=50
\lefthyphenmin=3
\righthyphenmin=3
\uchyph=0
\clubpenalty10000
\widowpenalty10000
\tolerance=1
\emergencystretch=\maxdimen
\pagestyle{headings}%
\pagenumbering{arabic}      % Arabic page numbers
\frenchspacing
\jot=2.5pt
\medmuskip=3.5mu
\thickmuskip=3.5mu
\thinmuskip=3.5mu
\sloppy%

%%%%%%%%%%%%%%%%%%%%%%%%% For table column alignments thorughout the journals %%%%%%%%%%%%%%%%%%%%%%%%%%%%%


\def\toprule{\\[-8.1pt]\topline\\[-2.9pt]}
\def\colrule{\\[-9.5pt]\midline\\[-7.5pt]}
\def\botrule{\iftablefootnotenone\\[-15.6pt]\else\\[-9.5pt]\botline\\[-6pt]\fi}
%


\newcommand{\orcid}[1][]{%
    \ifx\@orcid\@undefined%
    \else
            \ifpdf%
                \href{\@orcid}{{\includegraphics[width=9.1pt]{SSA_orcid_logo_Web.pdf}}}%
            \else
                \href{\@orcid}{{\includegraphics[width=9.1pt]{SSA_orcid_logo_Web.eps}}}%
            \fi
    \fi%
}

\newbox\recddatebox

\def\recdate#1{\gdef\@recdate{\fontfamily{\sfdefault}\fontseries{b}\fontsize{7}{12}\selectfont%\textcolor{color1}
{Manuscript Received #1}}}\recdate{Manuscript Received \the\day \the\month \the\year}


\def\enddata{%
\setbox\recddatebox\hbox{\@recdate}
\noindent{\vrule width\hsize height 0.5pt depth0pt}\rlap{\hspace*{-\wd\recddatebox}\raisebox{-14pt}{\@recdate}}
%\label{ArticleLastPage}
}

\def\enddocument{\enddata%
   \@kernel@before@enddocument
   \UseOneTimeHook{enddocument}%
   \@kernel@after@enddocument
   \@checkend{document}%
   \clearpage
   \UseOneTimeHook{enddocument/afterlastpage}%
   \@kernel@after@enddocument@afterlastpage
   \begingroup
     \if@filesw
       \immediate\closeout\@mainaux
       \let\@setckpt\@gobbletwo
       \let\@newl@bel\@testdef
       \@tempswafalse
       \makeatletter \@@input\jobname.aux
     \fi
     \UseOneTimeHook{enddocument/afteraux}%
     \UseOneTimeHook{enddocument/info}%
   \endgroup
   \UseOneTimeHook{enddocument/end}%
   \deadcycles\z@\@@end}

\bibliographystyle{SSAbib}

% \def\itemize{%
%   \ifnum \@itemdepth >\thr@@\@toodeep\else
%     \advance\@itemdepth\@ne
%     \edef\@itemitem{labelitem\romannumeral\the\@itemdepth}%
%     \expandafter
%     \list
%       \csname\@itemitem\endcsname
%       {\def\makelabel##1{{##1}}}%
%   \fi}

\def\enumerate{%
  \ifnum \@enumdepth >\thr@@\@toodeep\else
    \advance\@enumdepth\@ne
    \edef\@enumctr{enum\romannumeral\the\@enumdepth}%
      \expandafter
      \list
        \csname label\@enumctr\endcsname
        {\usecounter\@enumctr\def\makelabel##1{\hss\llap{##1}}}%
  \fi}
\let\endenumerate =\endlist
\newcount\@itemdepth \@itemdepth = 0
\def\itemize{%
  \ifnum \@itemdepth >\thr@@\@toodeep\else
    \advance\@itemdepth\@ne
    \edef\@itemitem{labelitem\romannumeral\the\@itemdepth}%
    \expandafter
    \list
      \csname\@itemitem\endcsname
      {\def\makelabel##1{\hss\llap{##1}}}%
      \leftskip-3pt%
  \fi}
\let\enditemize =\endlist


%%%%%%%%%%%%%%%%%% FLEQN %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\IncludeInRelease{2015/01/01}{\[}{Make \[ robust}%
\DeclareRobustCommand\[{\relax
                \ifmmode\@badmath
                \else
                  \begin{trivlist}%
                    \@beginparpenalty\predisplaypenalty
                    \@endparpenalty\postdisplaypenalty
                    \item[]\leavevmode
                    \hb@xt@\linewidth\bgroup $\m@th\displaystyle %$
                      \hskip\mathindent\bgroup
                \fi}
\EndIncludeInRelease
\IncludeInRelease{0000/00/00}{\[}{Make \[ robust}%
\renewcommand\[{\relax
                \ifmmode\@badmath
                \else
                  \begin{trivlist}%
                    \@beginparpenalty\predisplaypenalty
                    \@endparpenalty\postdisplaypenalty
                    \item[]\leavevmode
                    \hb@xt@\linewidth\bgroup $\m@th\displaystyle %$
                      \hskip\mathindent\bgroup
                \fi}
\EndIncludeInRelease
\IncludeInRelease{2015/01/01}{\]}{Make \] robust}%
\DeclareRobustCommand\]{\relax
                \ifmmode
                      \egroup $\hfil% $
                    \egroup
                  \end{trivlist}%
                \else \@badmath
                \fi}
\EndIncludeInRelease
\IncludeInRelease{0000/00/00}{\]}{Make \] robust}%
\renewcommand\]{\relax
                \ifmmode
                      \egroup $\hfil% $
                    \egroup
                  \end{trivlist}%
                \else \@badmath
                \fi}
\EndIncludeInRelease
\renewenvironment{equation}%
    {\@beginparpenalty\predisplaypenalty
     \@endparpenalty\postdisplaypenalty
     \refstepcounter{equation}%
     \trivlist \item[]\leavevmode
       \hb@xt@\linewidth\bgroup $\m@th% $
         \displaystyle
         \hskip\mathindent}%
        {$\hskip .3em minus.3em\hfil % $
         \displaywidth\linewidth\hbox{\@eqnnum}%
       \egroup
     \endtrivlist}
\renewenvironment{eqnarray}{%
    \stepcounter{equation}%
    \def\@currentlabel{\p@equation\theequation}%
    \def\@currentcounter{equation}%
    \global\@eqnswtrue\m@th
    \global\@eqcnt\z@
    \tabskip\mathindent
    \let\\=\@eqncr
    \setlength\abovedisplayskip{\topsep}%
    \ifvmode
      \addtolength\abovedisplayskip{\partopsep}%
    \fi
    \addtolength\abovedisplayskip{\parskip}%
    \setlength\belowdisplayskip{\abovedisplayskip}%
    \setlength\belowdisplayshortskip{\abovedisplayskip}%
    \setlength\abovedisplayshortskip{\abovedisplayskip}%
    $$\everycr{}\halign to\linewidth% $$
    \bgroup
      \hskip\@centering
      $\displaystyle\tabskip\z@skip{##}$\@eqnsel&%
      \global\@eqcnt\@ne \hskip \tw@\arraycolsep \hfil${##}$\hfil&%
      \global\@eqcnt\tw@ \hskip \tw@\arraycolsep
        $\displaystyle{##}$\hfil \tabskip\@centering&%
      \global\@eqcnt\thr@@
        \hb@xt@\z@\bgroup\hss##\egroup\tabskip\z@skip\cr}%
      {\@@eqncr
    \egroup
    \global\advance\c@equation\m@ne$$% $$
    \@ignoretrue
    }

%%%%%%%%%%%%%%%%%% FLEQN %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


\def\abc#1{\hangindent=125pt\hangafter=#1}

\def\narrowcol#1{\vspace*{-\baselineskip}\break

\global\narrowcoltrue
\abc{#1}\noindent}

\newcommand{\NCpar}[1]{%
\global\narrowcoltrue
\abc{#1}}


\def\pullback{\hspace*{-.5\textwidth}\hspace*{-.5\columnsep}}
\def\pulldown#1{%
\break\vspace*{-\baselineskip}\pagebreak

\vspace*{#1}\vspace*{.25\abovecaptionskip}
\noindent}

\def\Rnarrowcol{\vspace*{-\baselineskip}\break

\noindent\global\narrowcolfalse}
\def\mc#1#2#3{\multicolumn{#1}{#2}{#3}}

\newcommand\clearrows[1]{\pagebreak\null\newline\vspace*{#1\baselineskip}}

%\def\ang#1{${#1}^{\circ}$}

\setlength\columnsep{18\p@}
\setlength\columnseprule{0\p@}
%\pagestyle{plain}
\pagestyle{headings}%
\pagenumbering{arabic}
\if@twocolumn
  \twocolumn
  \sloppy
  \flushbottom
\else
  \onecolumn
\fi
\endinput
%%
%% End of file `article.cls'.
