# CRAN packages
pkgs <- c(
    "devtools",
    "viridis",
    "plotly",
    "flexdashboard",
    "knitr"
)
install.packages(pkgs)

# dev version of ggplot2
devtools::install_github("tidyverse/ggplot2")

# install Synapse
if (!require(synapseClient)) {
    source("http://depot.sagebase.org/CRAN.R")
    pkgInstall("synapseClient")
}
