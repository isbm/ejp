/**
 * Copyright (C) 2006 - present David Bulmore  
 * All Rights Reserved.
 *
 * This file is part of Easy Java Persistence.
 *
 * EJP is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the accompanying license 
 * for more details.
 *
 * You should have received a copy of the license along with EJP; if not, 
 * go to http://www.EasierJava.com and download the latest version.
 */

package ejp.utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

/**
 * A class for plurilization of nouns.
 *
 * <p>Based on the work:
 * <pre>
 *                An Algorithmic Approach to English Pluralization
 *
 *                                 Damian Conway
 *
 *              School of Computer Science and Software Engineering
 *                               Monash University
 *                            Clayton 3168, Australia
 *
 *                        mailto:damian@csse.monash.edu.au
 *                      http://www.csse.monash.edu.au/~damian
 * </pre>
 */
@SuppressWarnings("unchecked")
public class SimpleInflector
  {
    static String[] pluralSuffixes = new String[] { ".*fish",
                                                    ".*ois",
                                                    ".*sheep",
                                                    ".*deer",
                                                    ".*pox",
                                                    ".*[a-z].*ese",
                                                    ".*itis" };
    
    static Set ignoreInflection = arrayToSet(new String[] { "bison", "flounder", "pliers", "bream", "gallows", "proceedings",
                                                            "breeches", "graffiti", "rabies", "britches", "headquarters", "salmon",
                                                            "carp", "herpes", "scissors", "chassis", "highjinks", "seabass",
                                                            "clippers", "homework", "series", "cod", "innings", "shears",
                                                            "contretemps", "jackanapes", "species", "corps", "mackerel", "swine",
                                                            "debris", "measles", "trout", "diabetes", "mews", "tuna", "djinn",
                                                            "mumps", "whiting", "eland", "news", "wildebeest", "elk", "pincers" });
    
    static Map irregularNouns = arrayToMap(new Object[][] {{ "beef", new String[] { "beefs", "beeves" }},
                                                           { "brother", new String[] { "brothers", "brethren" }},
                                                           { "child", new String[] { "children" }},
                                                           { "cow", new String[] { "cows", "kine" }},
                                                           { "ephemeris", new String[] { "ephemerides" }},
                                                           { "genie", new String[] { "genies", "genii" }},
                                                           { "money", new String[] { "moneys", "monies" }},
                                                           { "mongoose", new String[] { "mongooses" }},
                                                           { "mythos", new String[] { "mythoi" }},
                                                           { "octopus", new String[] { "octopuses", "octopodes" }},
                                                           { "ox", new String[] { "oxen" }},
                                                           { "soliloquy", new String[] { "soliloquies" }},
                                                           { "trilby", new String[] { "trilbys" }},
                                                           { "loaf", new String[] { "loaves" }},
                                                           { "hoof", new String[] { "hoofs" }},
                                                           { "graffito", new String[] { "graffiti" }},
                                                           { "ganglion", new String[] { "ganglions" }},
                                                           { "turf", new String[] { "turfs" }},
                                                           { "numen", new String[] { "numina" }},
                                                           { "atman", new String[] { "atmas" }},
                                                           { "genus", new String[] { "genera" }},
                                                           { "occiput", new String[] { "occiputs" }}});

    static String[][] inflectionSuffixes = new String[][] {{ ".*man", "man", "men"},
                                                           { ".*[lm]ouse", "ouse", "ice" },
                                                           { ".*tooth", "tooth", "teeth" },
                                                           { ".*goose", "goose", "geese" },
                                                           { ".*foot", "foot", "feet" },
                                                           { ".*zoon", "zoon", "zoa" },
                                                           { ".*[csx]is", "is", "es" },
                                                           { ".*trix", "trix", "trices" },
                                                           { ".*eau", "eau", "eaux" },
                                                           { ".*ieu", "ieu", "ieux" },
                                                           { ".*..[iay]nx", "nx", "nges" },
                                                           { ".*[cs]h", "h", "hes" },
                                                           { ".*ss", "ss", "sses" },
                                                           { ".*[aeo]lf", "f", "ves" },
                                                           { ".*[^d]eaf", "f", "ves" },
                                                           { ".*arf", "f", "ves" },
                                                           { ".*[nlw]ife", "fe", "ves" },
                                                           { ".*[aeiou]y", "y", "ys" },
                                                           { ".*[A-Z].*y", "y", "ys" },
                                                           { ".*y", "y", "ies" },
                                                           { ".*[aeiou]o", "o", "os" },
                                                           { ".*o", "o", "oes" }};
                
    static Object[][] inflectionCategories = new Object[][] {{ arrayToSet(new String[] { "acropolis", "chaos", "lens", 
                                                                                         "aegis", "cosmos", "mantis",
                                                                                         "alias", "dais", "marquis", 
                                                                                         "asbestos", "digitalis", "metropolis",
                                                                                         "corpus", "opus", "penis", "testis", "atlas",
                                                                                         "epidermis", "pathos", "bathos", 
                                                                                         "ethos", "pelvis", "bias", "gas", 
                                                                                         "polis", "caddis", "glottis", "rhinoceros",
                                                                                         "cannabis", "glottis", "sassafras", 
                                                                                         "canvas", "ibis", "trellis" }), "s", "es" },
                                                             { arrayToSet(new String[] { "alumna", "alga", "vertebra" }), "a", "ae" },
                                                             { arrayToSet(new String[] { "abscissa", "formula", "medusa",
                                                                                         "amoeba", "hydra", "nebula",
                                                                                         "antenna", "hyperbola", "nova",
                                                                                         "aurora", "lacuna", "parabola" }), "a", "as", "ae" },
                                                             { arrayToSet(new String[] { "anathema", "enema", "oedema",
                                                                                         "bema", "enigma", "sarcoma",
                                                                                         "carcinoma", "gumma", "schema",
                                                                                         "charisma", "lemma", "soma",
                                                                                         "diploma", "lymphoma", "stigma",
                                                                                         "dogma", "magma", "stoma",
                                                                                         "drama", "melisma", "trauma",
                                                                                         "edema", "miasma" }), "a", "as", "ata" },
                                                             { arrayToSet(new String[] { "stamen", "foramen", "lumen" }), "en", "ens", "ina" },
                                                             { arrayToSet(new String[] { "codex", "murex", "silex" }), "ex", "ices" },
                                                             { arrayToSet(new String[] { "apex", "latex", "vertex",
                                                                                         "cortex", "pontifex", "vortex",
                                                                                         "index", "simplex" }), "ex", "exes", "ices" },
                                                             { arrayToSet(new String[] { "iris", "clitoris" }), "is", "ises", "ides" },
                                                             { arrayToSet(new String[] { "albino", "generalissimo", "manifesto",
                                                                                         "archipelago", "ghetto", "medico",
                                                                                         "armadillo", "guano", "octavo",
                                                                                         "commando", "inferno", "photo",
                                                                                         "ditto", "jumbo", "pro",
                                                                                         "dynamo", "lingo", "quarto",
                                                                                         "embryo", "lumbago", "rhino",
                                                                                         "fiasco", "magneto", "stylo" }), "o", "os" },
                                                             { arrayToSet(new String[] { "alto", "contralto", "soprano",
                                                                                         "basso", "crescendo", "tempo",
                                                                                         "canto", "solo" }), "o", "os", "i" },
                                                             { arrayToSet(new String[] { "aphelion", "hyperbaton", "perihelion",
                                                                                         "asyndeton", "noumenon", "phenomenon",
                                                                                         "criterion", "organon", "prolegomenon", }), "on", "a" },
                                                             { arrayToSet(new String[] { "agendum", "datum", "extremum",
                                                                                         "bacterium", "desideratum", "stratum",
                                                                                         "candelabrum", "erratum", "ovum" }), "um", "a" },
                                                             { arrayToSet(new String[] { "aquarium", "interregnum", "quantum",
                                                                                         "compendium", "lustrum", "rostrum",
                                                                                         "consortium", "maximum", "spectrum",
                                                                                         "cranium", "medium", "speculum",
                                                                                         "curriculum", "memorandum", "stadium",
                                                                                         "dictum", "millenium", "trapezium",
                                                                                         "emporium", "minimum", "ultimatum",
                                                                                         "enconium", "momentum", "vacuum",
                                                                                         "gymnasium", "optimum", "velum",
                                                                                         "honorarium", "phylum" }), "um", "ums", "a" },
                                                             { arrayToSet(new String[] { "focus", "nimbus", "succubus",
                                                                                         "fungus", "nucleolus", "torus",
                                                                                         "genius", "radius", "umbilicus",
                                                                                         "incubus", "stylus", "uterus" }), "us", "uses", "i" },
                                                             { arrayToSet(new String[] { "apparatus", "impetus", "prospectus",
                                                                                         "cantus", "nexus", "sinus",
                                                                                         "coitus", "plexus", "status",
                                                                                         "hiatus" }), "us", "uses", "us" },
                                                             { arrayToSet(new String[] { "afreet", "afrit", "efreet" }), "i" },
                                                             { arrayToSet(new String[] { "cherub", "goy", "seraph" }), "im" }};

    /**
     * This method pluralizes words (nouns only).  In the event of a multiple segment
     * word (camel case/underline) only the last segment will be pluralized.
     *
     * @param word the word to pluralize
     * @return returns an array of one or more pluralizations (modern followed by classic)
     */
    public static String[] pluralize(String word)
      {
        String[] words = parseWord(word), pluralizedWords = null;

        word = words[words.length - 1];

        if (doNotInflectPlural(word) || ignoreInflection.contains(word))
          pluralizedWords = new String[] { word };

        if (pluralizedWords == null)
          {
            if (irregularNouns.containsKey(word))
              pluralizedWords = (String[])irregularNouns.get(word);

            if (pluralizedWords == null)
              {
                Object[] category = getCategory(word);

                if (category != null)
                  pluralizedWords = getCategoryPlurals(category, word);

                if (pluralizedWords == null)
                  {
                    for (int i = 0; pluralizedWords == null && i < inflectionSuffixes.length; i++)
                      if (word.matches(inflectionSuffixes[i][0]))
                        pluralizedWords = new String[] { getInflection(word, inflectionSuffixes[i][1], inflectionSuffixes[i][2]) };

                    if (pluralizedWords == null)
                      pluralizedWords = new String[] { word + "s" };
                  }
              }
          }
        
        return repair(words, pluralizedWords);
      }

    static String[] parseWord(String word)
      {
        List words = new ArrayList();
        
        word = StringUtils.camelCaseToLowerCaseUnderline(word);
        StringTokenizer strtok = new StringTokenizer(word, "_");
        
        while (strtok.hasMoreTokens())
          words.add(strtok.nextToken());
        
        return (String[])words.toArray(new String[words.size()]);
      }

    static String[] repair(String[] segments, String[] pluralizedWords)
      {
        List pluralized = new ArrayList();
        String combined = "";
        
        for (int i = 0; i < segments.length - 1; i++)
          combined += (combined.length() > 0 ? "_" : "") + segments[i];
        
        if (combined.length() > 0)
          combined += "_";
        
        for (int i = 0; i < pluralizedWords.length; i++)
          pluralized.add(StringUtils.lowerCaseUnderlineToCamelCase(combined + pluralizedWords[i]));
                
        return (String[])pluralized.toArray(new String[pluralized.size()]);
      }
    
    static boolean doNotInflectPlural(String word)
      {
        for (int i = 0; i < pluralSuffixes.length; i++)
          if (word.matches(pluralSuffixes[i]))
            return true;
        
        return false;
      }
    
    static Object[] getCategory(String word)
      {
        for (int i = 0; i < inflectionCategories.length; i++)
          if (((Set)inflectionCategories[i][0]).contains(word))
            return inflectionCategories[i];
        
        return null;
      }
    
    static String[] getCategoryPlurals(Object[] category, String word)
      {
        String suffix = (String)category[1];
        List plurals = new ArrayList();
        
        if (category.length == 2)
          plurals.add(word + (String)category[1]);
        else
          {
            for (int i = 2; i < category.length; i++)
              plurals.add(getInflection(word,suffix,(String)category[i]));
          }
        
        return (String[])plurals.toArray(new String[plurals.size()]);
      }
    
    static String getInflection(String word, String suffix, String newSuffix)
      {
        return word.substring(0,word.length() - suffix.length()) + newSuffix;
      }
    
    static Set arrayToSet(Object[] array)
      {
        Set set = new HashSet();

        for (int i = 0; i < array.length; i++)
          set.add(array[i]);

        return set;
      }
    
    static Map arrayToMap(Object[][] array)
      {
        Map map = new HashMap();

        for (int i = 0; i < array.length; i++)
          map.put(array[i][0],array[i][1]);

        return map;
      }
  }
