# edgar-fetch
For downloading and accessing financial data from SEC's EDGAR

## To install dependencies in virtual environment with pip
Do the following at the root:

```bash
pip install -r requirements.txt
```
But before doing this, you might consider installing the whole package in the virtual environment, which would automatically install all required dependencies. 

## To install the package locally
Do the following after changing directory (cd) to the `edgar-fetch` root directory in the terminal:

```bash
pip install -e .
```

## Goal
To fetch SEC filings, both latest and historical, for credit risk reseacrch of companies in emerging markets especially the small and medium-size enterprises (SMEs). It is expected the focus will be on the major risk ratios: i.e. the debt, solvency, and profitabilty metrics. Particular attention should be paid to new markets in the retail and commercial segments of the startup landscape. We will therefore give less attention to the banking segment. 
