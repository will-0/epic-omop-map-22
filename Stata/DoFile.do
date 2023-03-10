******************************

* Import Data

import excel combined_data_11.19.2022.xlsx, firstrow case(lower)

******************************

* Relabel Variables

label define typelbl 0 "Element" 1 "Values"
encode type, gen(type2) label(typelbl)
order type2, a(type)
drop type
rename type2 type

label define equivlbl 0 "EQUAL" 1 "WIDER" 2 "NARROWER" 3 "UNMATCHED"
local users cc sb consensus wh
foreach x of local users {
	encode equivalence_`x', gen(equivalence_`x'_2) label(equivlbl)
	order equivalence_`x'_2, a(equivalence_`x')
	drop equivalence_`x'
	rename equivalence_`x'_2 equivalence_`x'
}

* Make another variable called equivalence_2 that just has 0 = EQUAL, 1 = WIDER or UNMATCHED or NARROWER

label define equivlbl2 0 "EQUAL" 1 "NOT EQUAL"
local users cc sb consensus wh
foreach x of local users {
	gen equivalence_2_`x' = equivalence_`x'
	replace equivalence_2_`x' = 1 if equivalence_2_`x' == 2
	replace equivalence_2_`x' = 1 if equivalence_2_`x' == 3
	label values equivalence_2_`x' equivlbl2
}


* Make a variable that expands on reasons for WIDER
label define widerlbl 1 "LATERALITY" 2 "CONCEPTMISSING" 3 "BOTH"
local users cc sb consensus wh
foreach x of local users {
	gen widerreason_`x' = 0 if equivalence_`x'==1
	replace widerreason_`x' = 1 if equivalence_`x'==1 & laterality_`x'==1 & conceptmissing_`x' == 0
	replace widerreason_`x' = 2 if equivalence_`x'==1 & laterality_`x'==0 & conceptmissing_`x' == 1
	replace widerreason_`x' = 3 if equivalence_`x'==1 & laterality_`x'==1 & conceptmissing_`x' == 1
	label values widerreason_`x' widerlbl
}


* Make a variable that expands on reasons for UMMATCHED
label define unmatchedlbl 1 "NOMATCH" 2 "VALSMAPPED" 3 "INDIRECT" 4 "SUBFIELD"
local users cc sb consensus wh
foreach x of local users {
	gen unmatchedreason_`x' = 0 if equivalence_`x'==3
	replace unmatchedreason_`x' = 1 if equivalence_`x'==3 & nomatch_`x'==1
	replace unmatchedreason_`x' = 2 if equivalence_`x'==3 & valsmapped_`x' == 1
	replace unmatchedreason_`x' = 3 if equivalence_`x'==3 & indirect_`x' == 1
	replace unmatchedreason_`x' = 4 if equivalence_`x'==3 & subfield_`x'==1
		label values unmatchedreason_`x' unmatchedlbl
}


* Label the languages
label define vocablbl 1 "SNOMED" 2 "LOINC" 3 "RxNorm" 4 "Other/None"
encode vocabulary_id_consensus, gen(vocabulary_id_consensus2) label(vocablbl1)
order vocabulary_id_consensus2, a(vocabulary_id_consensus)
drop vocabulary_id_consensus
rename vocabulary_id_consensus2 vocabulary_id_consensus

******************************

capture log close
log using dofile11.19.2022.txt, text replace

******************************

* Agreement between CC and SB using equivalence (EQUAL, WIDER, NARROWER, UNMATCHED)

kap equivalence_cc equivalence_sb

* Agreement between CC and SB using equivalence_2 (EQUAL, NOT EQUAL)

kap equivalence_2_cc equivalence_2_sb

* Agreement between CXC / SB among those labeled as NOT EQUAL, agreement for WIDER, NARROWER, UNMATCHED

kap equivalence_cc equivalence_sb if equivalence_2_cc == 1 & equivalence_2_sb == 1

* Agreement between CXC / SB among those both labeled as UNMATCHED, agreement for NO MATCH, VALSMAPPED, INDIRECT, SUBFIELD

kap unmatchedreason_cc unmatchedreason_sb if equivalence_cc == 3 & equivalence_sb == 3

* Agreement between CXC / SB on the exact concept ID for those were both EQUAL

kap conceptid_cc conceptid_sb if equivalence_2_cc == 0 & equivalence_2_sb == 0

* Agreement between CXC / SB on the exact concept ID for those were EQUAL, WIDER, or NARROWER

kap conceptid_cc conceptid_sb if equivalence_cc != 3 & equivalence_sb != 3

******************************

* Breakdown of EQUAL, WIDER, NARROWER, UNMATCHED
tab equivalence_2_consensus

tab equivalence_consensus

* WIDER: how many are laterality, concept missing, or both
tab widerreason_consensus

* Qualitative Description of Narrower

* UNMATCHED (NoMatch, ValsMapped, Indirect, Subfield) & Qualitative Description of Unmatched

tab unmatchedreason_consensus

* Breakdown by language (among EQUAL, WIDER, NARROWER): report #/% by SNOMED, LOINC, etc.

tab vocabulary_id_consensus if equivalence_consensus != 3

******************************

* Looking at Equivalence Changes by SB and CC from WH (flag 1 if either SB or CC made changes)
gen change_equiv_sborcc = 0 if equivalence_sb == equivalence_wh | equivalence_cc == equivalence_wh
replace change_equiv_sborcc = 1 if equivalence_sb != equivalence_wh | equivalence_cc != equivalence_wh
tab change_equiv_sborcc

kap equivalence_cc equivalence_sb if change_equiv_sborcc == 1 /* only look at kappa in rows where cc and sb made changes to equivalence*/

* Looking at ConceptID Changes by SB and CC from WH (flag 1 if either SB or CC made changes)
gen change_conceptid_sborcc = 0 if conceptid_sb == conceptid_wh | conceptid_cc == conceptid_wh
replace change_conceptid_sborcc = 1 if conceptid_sb != conceptid_wh | conceptid_cc != conceptid_wh
tab change_equiv_sborcc

kap conceptid_cc conceptid_sb if equivalence_cc != 3 & equivalence_sb != 3 & change_conceptid_sborcc == 1 /* look at agreement in exact concept ID among concepts that had some sort of match (equal, wider, or narrower), and sb or cc made a change in conceptid*/

* Looking at Either Equivalence or ConceptID Changes by SB and CC from WH
gen change_equiv_conceptid_sborcc = 0 if change_equiv_sborcc == 0 | change_conceptid_sborcc == 0
replace change_equiv_conceptid_sborcc = 1 if change_equiv_sborcc == 1 | change_conceptid_sborcc == 1

kap equivalence_cc equivalence_sb if change_equiv_conceptid_sborcc == 1/* only look at kappa in rows where cc and sb made either changes to equivalence or concept id*/

kap conceptid_cc conceptid_sb if equivalence_cc != 3 & equivalence_sb != 3 & change_equiv_conceptid_sborcc == 1 /* look at agreement in exact concept ID among concepts that had some sort of match (equal, wider, or narrower), and sb or cc made changes to equivalence or concept id */


* Look at number of equivalence or conceptID changes made by SB and CC from WH
tab change_equiv_conceptid_sborcc

* Look at number of equivalence or conceptID changes made by SB from WH
tab change_equiv_conceptid_sborcc if equivalence_sb != equivalence_wh | conceptid_sb != conceptid_wh


* Look at number of equivalence or conceptID changes made CC from WH
tab change_equiv_conceptid_sborcc if equivalence_cc != equivalence_wh | conceptid_cc != conceptid_wh






