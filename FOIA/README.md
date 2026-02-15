<h1>U.S. Department of Labor Top Hat Plan data</h1>

<p>What this repository contains:</p> 
<ol>
  <li>Top Hat Plan data, obtained via FOIA request to the U.S. Department of Labor</li>
  <li>That data, supplemented with links to additional sources of employer information (specifically, Paycheck Protection Program and WARN alert data points)</li>
</ol>

<p>As mentioned, the centerpiece data in this repository comes from the U.S. Department of Labor. A <a href="https://www.askebsa.dol.gov/tophatplansearch" target="_blank">limited-feature search portal</a> is available on DOL's website (as of 12/4/2021) to download "top hat statement" pdf files. However, this portal lacks contextual information (<a href="https://www.askebsa.dol.gov/efile/Home/tophat" target="_blank">input via web form completed by employers</a>). Thankfully, that additional contextual information is available in a dataset format.</p>

Using this data, _The Chronicle_ published the story <a href="https://www.chronicle.com/article/just-rewards" target="_blank">"These Colleges Cut Back During the Pandemic. Except When It Came to Their Presidents’ Pay Packages."</a>

<p><h2><strong>What are Top Hat Plans?</strong></h2></p>

<p>Top hat plans are "unfunded, nonqualified deferred compensation plans covering a 'select group of management or highly compensated employees.'"<sup>1</sup> </p>

<p>"Top hat pension plans are a type of nonqualified deferred compensation (NQDC) arrangement. Employers maintain NQDC plans for a number of reasons, including recruiting employees, incentivizing employees to remain employed for a specified period (sometimes referred to as a 'golden handcuffs' arrangement), and supplementing retirement benefits provided under tax-qualified retirement plans."<sup>1</sup></p>

<p><h2><strong>Okay... What Is The Difference Between a "Qualified" Deferred Compensation Plan and a "Nonqualified" Plan?</strong></h2></p>

<p>A "qualified" deferred compensation plan is a pension plan governed under the Employee Retirement Income Security Act (ERISA).<sup>2</sup> Contributions made to a "qualified" deferred compensation plan are put into a trust that is separate from an employer's operations.<sup>3</sup> Traditional 401(k)s or 403(b)s are types of "qualified" deferred compensation plans.<sup>3</sup></p>

<p>A "nonqualified" deferred compensation plan works differently. Compensation contributed to these plans is considered part of an employer's assets, and can be used by that employer for business reasons.<sup>3</sup> And as described above, "nonqualified" deferred compensation plans are designed to incentivize employee longevity. In practice, this means if an employee fails to satisfy the terms of a "nonqualified" deferred-compensation contractual arrangement, the employer does not owe compensation to that employee.</p>

<p><h2><strong>I Still Don't Get It</strong></h2></p>

<p>As an example, consider President Sarah Brown at Hoover University. Let's say President Brown's employment contract results in the establishment, in 2020, of a "nonqualified" deferred compensation plan for her. And Hoover University is contractually obligated to annually credit $100,000 into this "nonqualified" deferred compensation plan. Specifically, that crediting will occur respectively on July 1st of each of the following years: 2021, 2022, 2023, 2024, and 2025 — as long as Brown is still employed respectively at Hoover U. on each of those five dates.</p>

<p>President Brown's deferred-compensation arrangement also sets the plan's vesting date as July 1, 2025 — but again only if Brown is still an employee of Hoover University on that date. No retiring early. No leaving for a cooler job somewhere else. To be owed the money that will be credited towards her deferred compensation plan, Brown must still be an employee of Hoover U. on July 1, 2025. 

<p>When 7/1/2025 arrives, if Brown is still an employee of Hoover U., HU will owe Brown at least $500,000 — compensation that Hoover University has been contractually crediting towards Brown's "nonqualified" deferred compensation plan since 2021. This compensation is likely in addition to the base compensation that Brown is traditionally paid every year, alongside any performance-based bonuses, allowances, or other perks. 

<p>But if Brown separates from Hoover U. at any time before 7/1/2025, HU no longer needs to credit money towards Brown's "nonqualified" deferred compensation plan. Nor does Hoover U. need to anticipate owing Brown any taxable compensation in 2025.</p>

<p><h2><strong>Okay... What Makes a Nonqualified Deferred Compensation Plan "Unfunded" Instead of "Funded?"</strong></h2></p>

<p>The "unfunded" or "funded" nature of a plan indicates how its future payments will be financed.</p>

<p>When a deferred compensation plan is "funded," an employer is actively and systemically setting aside cash in advance to cover the employer's obligations as defined by the deferred-compensation contractual arrangement.<sup>4</sup></p>

<p>When a deferred compensation plan is "unfunded," the payments made by virtue of the plan are originating from the current income generated by the employer. No money is being set aside in advance. Or if money is being set aside, those assets can be subject to claims by general creditors. "In general, in an unfunded plan, your employees rely solely on your unsecured promise to pay benefits at a later date"<sup>4</sup></p> 

<p><h2><strong>What Else Should I Know About This Data?</strong></h2></p>

<p>Only non-government entities are required to submit "top hat statements" to DOL. That includes publicly traded companies, privately held companies, and private, non-profit companies (hereafter all referred to collectively as "private companies").</p>

<p>To avoid all the regulation and scrutiny that comes with ERISA, these private companies must submit a "top hat statement" to DOL within 120 days of the top hat plan's establishment date. In practice, this "statement" takes the form<sup>1</sup> of:</p>

<ul>
  <li>the name and address of the employer;</li>
  <li>the employer identification number (EIN);</li>
  <li>a declaration that the employer maintains plan(s) primarily for the purpose of providing
  deferred compensation for a select group of management or highly compensated
  employees; and</li>
  <li>a statement of the number of such plans and the number of employees in each plan.</li>
</ul>

<p>Reporting note: <i>When The Chronicle interviewed the representatives of university-employers that established top hat plans for their presidents, these representatives sought to create a distinction between a top hat plan's purported "establishment date" and the time period during which the same top hat plan was conceived of — usually as a result of contract negotiations between a university's governing board and its president. All representatives claimed the "establishment" of a plan was a mere formality, and that The Chronicle should use pre-pandemic contract extensions or negotiations as a more accurate point of comparison in our reporting.</i></p>

<p><h2><strong>Should I Download The Actual Top Hat Statements?</strong></h2></p>

<p>Yes!</p> 

<p>For whatever reason, the top hat dataset obtained from DOL fails to include all of the contextual information present in electronically submitted top hat statements. For instance, consider this row from the dataset, which concerns a filing from Assumption University, dated 9/9/2020:</p> 

<ul>
  <li>Filing Date: 9/9/2020</li>
  <li>Filing Type: Tophat</li>
  <li>Filing Method: Electronic</li>
  <li>Sponsor EIN: 42105776</li>
  <li>Filing Name: Assumption University</li>
  <li>Plan Name or Paper Filing Notes: Assumption University 457(f) Deferred Compensation Plan</li>
  <li>Employer Name: Assumption University</li>
  <li>Employer Address 1: 500 Salisbury Street</li>
  <li>Employer Address 2:</li>
  <li>Employer City: Worcester</li>
  <li>Employer State: MA</li>
  <li>Employer Zip Code: 01609</li>
  <li>Participants: 1</li>
  <li>No. of Plans in Filing: 1</li>
  <li>No. of Filings: 1</li>
</ul>
  
<p>But when you download the <a href="https://www.askebsa.dol.gov/tophatplansearch/Home/DownloadPdf?id=000000006927&form_type=Top%20Hat">actual statement pdf file</a> from DOL's portal, we learn some additional information about this plan. There's the name of the plan administrator, their contact information, and ...</p>

> "The Plan is effective as of July 1, 2020 and was adopted on September 8, 2020. The Plan has one participant (Francesco C. Cesareo, Ph.D.)."

<p>Francesco C. Cesareo is the president of Assumption University. Obviously, that is some very important information for my reporting that I wouldn't have known about had I not downloaded the actual statement.</p>

<p><h2><strong>What's This Stuff About PPP Loans and WARN Alerts?</strong></h2></p>

<p>Cognizant of the valuable information contained in this dataset, I took the time to query (by keyword) two additional sources of information before I published it here on Github. I added links to:</p>
<ol>
  <li> Hits I got using ProPublica's <a href="https://projects.propublica.org/coronavirus/bailouts/" target="_blank">searchable database of Small Business Administration PPP loans</a></li>
  <li> Hits I got searching through WARN alerts published by 40+ states for 2020 and 2021</li>
</ol>

<p><h2><strong>Anything Else?</strong></h2></p>

<p>I can't stress this enough: <b>Conduct your own due dillegence when it comes to these PPP and WARN alerts supplements!</b></p>

<p><h2><strong>Why Should I?</strong></h2></p>

<p>Because...</p>

<p><b>A:</b> My searches were cursory;</p>

<p><b>A:</b> My appetite for entertaining false positives was greater than I'd stomach otherwise, given that I didn't intend to write about all of these datapoints or widely PUBLISH this dataset on <i>The Chronicle</i>'s website;</p>

<p><b>A:</b> I didn't care to learn about the technical nuances of each state's WARN-alert systems. So I can't tell you why some states record even minor layoff events, while others barely track any;</p>

<p><b>A:</b> Because of the time, learning curve, and grunt work involved, I didn't integrate additional datapoints into this file, like data about federal Covid-relief dollars allocated to specific industries. Think airlines, colleges, or hospitals.</p> 

<p><b>A:</b> I also couldn't conceptualize an effective and comprehensive strategy for identifying other pandemic-era workforce events. All I could come up with was:</p>
<ul>
  <li>Do a billion Google searches with different keywords AND 'company name.'</li>
  <li>Never see family or friends again.</li>
</ul>

<p>So, yeah, I passed on that;</p>

<p><b>A:</b> I did integrate PPP loan "keyword" matches into this dataset. You <b>SHOULD NOT</b> consider that integration a blanket endorsement <b>FROM ME</b> to <b>UNCRITICALLY CITE</b> the existence of Covid-relief information within any final editorial product you generate.</p>

<p>We had a vigorous debate at <i>The Chronicle</i> about what it meant for a college or university to seek out and/or accept federal relief during this pandemic. You should undertake a similar debate within yourself and/or your own newsroom if you are considering using PPP data, WARN alert data, or other supplementary data when reporting on these plans — even if you ultimately arrive at a different decision than <i>The Chronicle</i> did. Ultimately, the references to PPP data remain present in the file because it's possible additional reporting and context from you may justify its reference in a final editorial product. 

<p><h2><strong>Questions / Feedback / Improvements</strong></h2></p>

<p>Email the repository's maintainer at dan.bauman@chronicle.com.</p>

<p><h2><strong>Sources</strong></h2></p>

1. https://www.dol.gov/sites/dolgov/files/EBSA/about-ebsa/about-us/erisa-advisory-council/2020-examining-top-hat-plan-participation-and-reporting.pdf

2. https://satoriwealth.com/nonqualified-deferred-compensation-plans/

3. https://www.thehartford.com/business-insurance/strategy/deferred-compensation-plans/nonqualified

4. https://www.henssler.com/nqdc-plans-funding-options/
