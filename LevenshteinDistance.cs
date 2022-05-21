using System;

namespace DMSModelConfigDbUpdater
{
    /// <summary>
    /// Calculates the number of letter additions, subtractions, substitutions,
    /// and transpositions (swaps) necessary to convert one string to another.
    /// The lower the score, the more similar they are.
    /// </summary>
    internal static class LevenshteinDistance
    {
        // Ignore Spelling: Damerau, jth, Levenshtein

        /// <summary>
        /// Compute the Damerau-Levenshtein distance between two strings
        /// </summary>
        /// <remarks>Based on https://stackoverflow.com/a/57961456/1179467</remarks>
        /// <param name="s"></param>
        /// <param name="t"></param>
        /// <returns>Distance (lower value means more similar)</returns>
        public static int GetDistance(string s, string t)
        {
            if (string.IsNullOrEmpty(s))
            {
                return string.IsNullOrEmpty(t) ? 0 : t.Length;
            }

            if (string.IsNullOrEmpty(t))
            {
                return s.Length;
            }

            // Cache the string lengths
            var n = s.Length;
            var m = t.Length;

            var p = new int[n + 1]; // 'previous' cost array, horizontally
            var d = new int[n + 1]; // cost array, horizontally

            // Indexes into strings s and t
            // i iterates through s
            // j iterates through t
            int i;
            int j;

            for (i = 0; i <= n; i++)
            {
                p[i] = i;
            }

            for (j = 1; j <= m; j++)
            {
                // tJ is the jth character of t
                var tJ = t[j - 1];
                d[0] = j;

                for (i = 1; i <= n; i++)
                {
                    var cost = s[i - 1] == tJ ? 0 : 1;

                    // Minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                    d[i] = Math.Min(Math.Min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
                }

                // Copy current distance counts to 'previous row' distance counts
                // Swap via deconstruction
                (p, d) = (d, p);
            }

            // Our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts
            return p[n];
        }
    }
}
