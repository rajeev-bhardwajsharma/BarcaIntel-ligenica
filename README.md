#  FC Barcelona: A Quantitative Look at Tactical Evolution (Enrique & Valverde)

Football has always been full of opinions but what if we let the *data* do the talking?

This project dives into **how FC Barcelona’s tactical identity evolved** between the eras of **Luis Enrique (2014–2017)** and **Ernesto Valverde (2017–2020)**.  
Instead of relying on emotion or hindsight, it uses **event level data from StatsBomb** to map out how style, structure, and player roles shifted during those years.

It’s not about proving who was better it’s about understanding *how* the team changed, one pass, one carry, and one xG at a time.

---

##  Project Intent

The goal is to build a **machine learning framework** that can *recognize and measure* tactical fingerprints in football.  
Rather than manually labeling tactics, the project explores whether algorithms can discover those patterns themselves.

A few key questions guided this work:

- Can we define a team’s tactical **“personality”** through their possession data?
- Can **unsupervised learning** uncover meaningful, interpretable player roles?
- How did Barcelona’s overall strategy  and its key players’ responsibilities  evolve from Enrique’s fluid, high tempo style to Valverde’s more controlled, positional play?

At its heart, this project is an attempt to **quantify football intelligence**  to turn intuition into insight.

---

##  Methodology & Tools

This analysis blends **feature engineering** with **unsupervised learning**, allowing the data itself to reveal its internal logic.

### Data Source
- **StatsBomb Open Data**  rich event level match data with granular details on passes, carries, pressures, and more.

###  Feature Engineering Highlights
- Per 90 minute normalizations for fair player comparison  
- Progressive pass & carry distances  
- Average shot quality (xG)  
- Possession tempo & directness metrics  
- Defensive intensity features  

###  Machine Learning Models
- **K-Means Clustering:** to identify core possession styles and player roles  
- **Gaussian Mixture Models (GMM):** to detect “hybrid” player behaviors  
- **Isolation Forest:** to find statistically unique or standout players  
- **Hierarchical Clustering:** to map relationships between player roles  
- **t-SNE:** for dimensionality reduction and visual exploration of clusters  

###  Key Libraries
`pandas`, `scikit-learn`, `matplotlib`, `seaborn`, `mplsoccer`

---

##  Repository Structure

| Notebook | Description |
|-----------|--------------|
| **Attack_analysis.ipynb** | Exploratory breakdown of attacking dynamics and shot creation |
| **Defensive_approach_analysis.ipynb** | Analysis of defensive pressure, tackles, and positioning |
| **Possession_Style_Directness.ipynb** | K-Means module analyzing team-level possession styles |
| **Barca_Tactical_Fingerprints.ipynb** | Main ML module for player role clustering (K-Means, GMM, Isolation Forest, visualization) |

---

## Closing Thoughts

This project isn’t about nostalgia or debate  it’s about **discovery**.  
When you strip away opinions and visuals, football becomes patterns  interconnected movements that tell their own story.

Here, the numbers don’t argue.  
They reveal.

---

**Author:** Rajeev Sharma 
**Data:** StatsBomb Open Data  
**Built With:** Curiosity, Python, and way too many late night coffees ☕
