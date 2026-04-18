"""Saved BSE JSON fixtures for ownership endpoint testing."""

SHAREHOLDING_FIXTURE = [
    {
        "SHPDate": "31 Mar 2026",
        "PromoterPer": "50.30",
        "PromoterPledgedPer": "2.15",
        "PublicPer": "49.70",
        "FIIPer": "23.50",
        "DIIPer": "15.80",
        "InsurancePer": "4.20",
        "MFPer": "8.10",
        "RetailPer": "6.50",
        "BodyCorpPer": "3.60",
        "TotalShareholders": "3456789",
    },
    {
        "SHPDate": "31 Dec 2025",
        "PromoterPer": "50.50",
        "PromoterPledgedPer": "2.30",
        "PublicPer": "49.50",
        "FIIPer": "23.00",
        "DIIPer": "16.10",
        "InsurancePer": "4.10",
        "MFPer": "7.90",
        "RetailPer": "6.80",
        "BodyCorpPer": "3.70",
        "TotalShareholders": "3400100",
    },
    {
        "SHPDate": "30 Sep 2025",
        "PromoterPer": "50.80",
        "PromoterPledgedPer": "2.50",
        "PublicPer": "49.20",
        "FIIPer": "22.70",
        "DIIPer": "16.30",
        "InsurancePer": "4.00",
        "MFPer": "7.80",
        "RetailPer": "7.00",
        "BodyCorpPer": "3.50",
        "TotalShareholders": "3350000",
    },
]

SHAREHOLDING_ALT_KEYS_FIXTURE = [
    {
        "shpDate": "30 Jun 2025",
        "promoterPer": "51.00",
        "promoterPledgedPer": "1.80",
        "publicPer": "49.00",
        "fiiPer": "22.50",
        "diiPer": "16.50",
        "insurancePer": "3.90",
        "mfPer": "7.70",
        "retailPer": "7.20",
        "bodyCorpPer": "3.40",
        "totalShareholders": "3300000",
    },
]

PLEDGE_FIXTURE = [
    {
        "Date": "31 Mar 2026",
        "PromoterHolding": "340500000",
        "PromoterPledged": "7321750",
        "PledgedPer": "2.15",
        "TotalShares": "677100000",
    },
    {
        "Date": "31 Dec 2025",
        "PromoterHolding": "341850000",
        "PromoterPledged": "7862550",
        "PledgedPer": "2.30",
        "TotalShares": "677100000",
    },
]

PLEDGE_ALT_KEYS_FIXTURE = [
    {
        "PLEDGEDATE": "30 Sep 2025",
        "PROMOTER_HOLDING": "343200000",
        "PROMOTER_PLEDGED": "8580000",
        "PLEDGED_PER": "2.50",
        "TOTAL_SHARES": "677100000",
    },
]

INSIDER_FIXTURE = [
    {
        "PERSONNAME": "Mukesh D Ambani",
        "CATEGORY": "Promoter",
        "ACQMODE": "Market Purchase",
        "SECACQ": "150000",
        "TDPTVALUE": "18750000000",
        "ACQUISITIONFROMDATE": "10 Apr 2026",
        "INTIMATEDDT": "12 Apr 2026",
    },
    {
        "PERSONNAME": "Nita M Ambani",
        "CATEGORY": "Promoter Group",
        "ACQMODE": "Market Sale",
        "SECACQ": "50000",
        "TDPTVALUE": "6250000000",
        "ACQUISITIONFROMDATE": "08 Apr 2026",
        "INTIMATEDDT": "10 Apr 2026",
    },
    {
        "PERSONNAME": "Srinivas Murthy",
        "CATEGORY": "KMP",
        "ACQMODE": "ESOP",
        "SECACQ": "5000",
        "TDPTVALUE": "625000000",
        "ACQUISITIONFROMDATE": "05 Apr 2026",
        "INTIMATEDDT": "07 Apr 2026",
    },
    {
        "PERSONNAME": "Ramesh Director",
        "CATEGORY": "Director",
        "ACQMODE": "Pledge Creation",
        "SECACQ": "100000",
        "TDPTVALUE": "12500000000",
        "ACQUISITIONFROMDATE": "01 Apr 2026",
        "INTIMATEDDT": "03 Apr 2026",
    },
]

INSIDER_ALT_KEYS_FIXTURE = [
    {
        "personName": "Alt Key Person",
        "category": "Promoter",
        "acqMode": "Market Purchase",
        "secAcq": "25000",
        "tdptValue": "3125000000",
        "acqfromDt": "15 Mar 2026",
        "intimDt": "17 Mar 2026",
    },
]

SAST_FIXTURE = [
    {
        "ACQUIRERNAME": "Reliance Strategic Investments Ltd",
        "ACQUIRERTYPE": "Promoter Group",
        "PREHOLDING": "48.50",
        "POSTHOLDING": "50.30",
        "TRANSACTIONDATE": "15 Mar 2026",
        "DISCLOSUREDATE": "17 Mar 2026",
        "REGULATION": "Reg 7(1)",
    },
    {
        "ACQUIRERNAME": "BlackRock Fund Advisors",
        "ACQUIRERTYPE": "FPI",
        "PREHOLDING": "4.80",
        "POSTHOLDING": "5.10",
        "TRANSACTIONDATE": "10 Mar 2026",
        "DISCLOSUREDATE": "12 Mar 2026",
        "REGULATION": "Reg 10(7)",
    },
    {
        "ACQUIRERNAME": "Vanguard Emerging Markets",
        "ACQUIRERTYPE": "FPI",
        "PREHOLDING": "5.20",
        "POSTHOLDING": "4.90",
        "TRANSACTIONDATE": "05 Mar 2026",
        "DISCLOSUREDATE": "07 Mar 2026",
        "REGULATION": "Reg 7(2)",
    },
]

SAST_ALT_KEYS_FIXTURE = [
    {
        "acquirerName": "Alt Acquirer Ltd",
        "acquirerType": "Promoter",
        "preHolding": "25.00",
        "postHolding": "26.50",
        "transDate": "01 Feb 2026",
        "disclosureDate": "03 Feb 2026",
        "regulation": "Reg 7(1)",
    },
]
