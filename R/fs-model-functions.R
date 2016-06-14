sprod <- function(x, y) {
    prod(x^y * (1-x)^(1-y))
}

expect <- function(p, m, u, gamma_j) {
    numerator <- p * sprod(m, gamma_j)
    denom_piece <- (1-p) * sprod(u, gamma_j) 
    denom <- numerator + denom_piece
    numerator/denom
}

match_prob <- function(gamma, preds = names(gamma), p, m, u) {
    # as input: a DATA FRAME of 1/0, plus a column called "count"
    # as output: same data frame with extra columns: numeric vector g
    # assert that:
    # gamma is a data frame
    # all columns in preds are of type NUMERIC
    # all values in columns preds are 1 or 0
    # m and u have the same length and names
    # m, u, and gamma[,preds,drop=F] have the same names
    # ncol(gamma[,preds,drop=F]) = length(m) = length(u)
    # m and u are numeric
    # max of m/u < 1
    # min of m/u > 0
    # all(m > u)
    # p is numeric(1)
    
    apply(gamma[, preds, drop=FALSE], 1, function(x) expect(p, m, u, x))
}

max_m <- function(gamma, preds, count, g) {
    # length(g) == nrow(gamma) == length(count), etc...
    count <- gamma[[count]]
    g <- gamma[[g]]
    weighted_g <- g * count
    vapply(gamma[ , preds, drop=FALSE], 
           function(x) sum(x * weighted_g)/sum(weighted_g), 
           FUN.VALUE = numeric(1))
}

max_u <- function(gamma, preds, count, g) {
    # length(g) == nrow(gamma) == length(count), etc...
    count <- gamma[[count]]
    g <- gamma[[g]]
    weighted_ung <- (1-g) * count
    vapply(gamma[ , preds, drop=FALSE], 
           function(x) sum(x * weighted_ung)/sum(weighted_ung), 
           FUN.VALUE = numeric(1))
}

random_agreement <- function(gamma, preds, count) {
    vapply(gamma[ , preds, drop=FALSE], 
           function(x) weighted.mean(x, gamma[[count]]), 
           FUN.VALUE = numeric(1))
}

fs_weights <- function(df, preds, count, p, m, u, tolerance = .001) {
    df2 <- df %>%
        mutate(g = match_prob(., preds, p, m, u))
    pnew <- weighted.mean(df2$g, df2$count)
    mnew <- max_m(df2, preds, count, "g")
    unew <- max_u(df2, preds, count, "g")
    if (sum(abs(m - mnew)) <= (tolerance * length(m)) &&
        sum(abs(u - unew)) <= (tolerance * length(u)) &&
        abs(p - pnew)  <= tolerance) {
        return(
            structure(
                list(
                    m = mnew,
                    u = unew,
                    p = pnew)))
    } else {
        return(fs_weights(df, preds, count, pnew, mnew, unew, tolerance))
    }
}