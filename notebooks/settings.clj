(ns settings
  "Clerk notebook illustrating use of `witan.send.settings`"
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.gias :as gias]
            [witan.send.settings :as settings]))

^::clerk/no-cache
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# witan.send.settings"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)
               "  \nTimeStamp: " (.format (java.time.LocalDateTime/now)
                                          (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE_TIME))))
{::clerk/visibility {:code :show}}


;;; # Settings
;;; ## TL;DR
;; To get the setting for a sen2-estab
;; {`:urn` `:ukprn` `:sen-unit-indicator` `:resourced-provision-indicator` `:sen-setting`}
;; using the MC standard settings from the library resource folder,
;; with LA funded establishment categories split into in vs. out of
;; area for local LA code "879":


;;; ### configuration
;; First, get the config map for the standard MC settings with the set of local LA codes added in:
^{::clerk/visibility {:result :hide}}
(def standard-cfg-for-879
  (settings/standard-cfg ::settings/in-area-la-codes #{"879"}))

;; This includes the GIAS lookup loaded from the edubaseall CSV, so it may take a second to build, and is quite big.


;;; ### lookup setting
;; Use `settings/sen2-estab->setting` to get the setting, passing the `sen2-estab` map and config as parameters:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn                           "113644"
  :ukprn                         nil
  :sen-unit-indicator            false
  :resourced-provision-indicator false
  :sen-setting                   nil}
 standard-cfg-for-879)


;;; ### more details
;; Use `settings/sen2-estab->setting-map` to get a map back with more information. Keys returned are:
(keys (settings/sen2-estab->setting-map {:urn "113644"} standard-cfg-for-879))

;; Of which `:estab-name` and `:setting` may be the most useful to retain in analysis:
(select-keys (settings/sen2-estab->setting-map {:urn "113644"} standard-cfg-for-879)
             [:estab-name :setting])


;;; ### Setting definitions
;; Details on (all) the settings is in the config, Definitions of the
;; three `estab-cat`, `designation` & `area` components are contained
;; in the config, and can be extracted as datasets or maps with
;; correspondingly named functions. For example:
^{::clerk/viewer clerk/table}
(-> (settings/estab-cats-ds standard-cfg-for-879)
    (tc/select-columns [:abbreviation :label :designate? :split-area?]))

;; The `settings/settings` function expands the estab-cats to be
;; designated and split for area by the designation and area
;; abbreviations to give a complete lookup of all (possible)
;; settings as a map. …and `settings/settings-ds` function as a dataset:
^{::clerk/viewer    clerk/table
  ::clerk/page-size 16}
(-> (settings/settings-ds standard-cfg-for-879)
    (tc/select-columns [:abbreviation :order :label]))


;;; ### XxX & UKN!
;; If you get "XxX" as part of the returned setting abbreviations then you need to provide more information:
;; - "XxX" for the first `estab-cat` component indicates can't get details via GIAS establishment type:
;;   - if not in GIAS or a "Welsh establishment" then use manual specification of settings.
;;   - if in GIAS then check mapping of establishment types to settings.
;; - "XxX" for the second `designation` component indicates that the `designation-f` returned `nil`:
;;   - check the designation-f.
;; - "XxX" for the final `area` component indicates that the `area-split-f` returned `nil`:
;;   - check the area-split-f and specification of set of `in-area-la-codes`.

;; A `sen2-estab` with all components falsey will return a setting of "UKN":
;; - Check placement data!


;;; ### Plus…
;; - `settings/setting-split-regexp` returns a regex that will extract
;;   the `estab-cat`, `designation` and `area` abbreviations from a
;;   `setting` abbreviation.
;; - Configuration is via `cfg` map argument (or trailing key-value
;;   pairs).
;; - Lookups (e.g. for setting component definitions or mappings) are
;;   used as maps but can be specified as maps, datasets or CSV files.
;; - If specifying by files then recommend using `settings/parse-cfg`
;;   to read the files into the config, as otherwise the files will be
;;   read on every invocation.

;; For details, read on…



;;; ## Settings via GIAS
;; Given a `sen2-estab` (map) and settings `cfg` map, function
;; `sen2-estab->setting` returns the setting.


;;; ### `estab-cat`
;; The establishment category `estab-cat` is the first component of the setting.

;;; #### GIAS: URN to estab-type
;; Establishments are first mapped to their GIAS establishment type by
;; looking up their URN in the mapping of URNs to GIAS "edubaseall"
;; SEND attributes:
;; - The map must be specified in the `::settings/edubaseall-send-map` config key-value pair.
;; - `(witan.gias/edubaseall-send->map)` returns a map derived from the current edubaseall.csv file.
;;   (Other edubaseall files can be specified in options.)
;; - Note that the edubaseall.csv is quite large at about 65MB, and may take a second to read.

^{::clerk/visibility {:result :hide}}
(def edubaseall-send-map (gias/edubaseall-send->map))

;; - The `settings/parse-cfg` function will
;;   call `(witan.gias/edubaseall-send->map cfg)` and add the map
;;   returned to the `::settings/edubaseall-send-map` key-value pair
;;   of the config.

;;; #### `estab-type` to `estab-cat`
;; Once the GIAS `:type-of-establishment-name` has been looked up,
;; this is combined into a with the `:sen-unit-indicator`,
;; `:resourced-provision-indicator` & `:sen-setting` to give an
;; `estab-type` map, from which the `estab-cat` is determined via
;; lookup: The lookup for this is specified in the
;; `::settings/estab-type-to-estab-cat` config key:
;; - as a map
;; - as a dataset
;; - or via CSV filepath

;; Here's a simple example of a `::settings/estab-type-to-estab-cat`
;; lookup for a single estab-type (as a dataset):
^{::clerk/viewer clerk/table}
(def estab-type-to-estab-cat-ds-1
  (tc/dataset [{:type-of-establishment-name    "Foundation special school"
                :sen-unit-indicator            false
                :resourced-provision-indicator false
                :sen-setting                   nil
                :estab-cat                     "SpMdA"}]))


;; …which we can use to get the setting for a `sen2-estab` as follows:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn                           "113644"
  :ukprn                         nil
  :sen-unit-indicator            false
  :resourced-provision-indicator false
  :sen-setting                   nil}
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-1)

;;; #### Partial `sen2-estab` maps
;; The `sen2-estab` map is destructured within `sen2-estab->setting`
;; and missing/nil keys set to `nil` (`:urn`, `:ukprn` &
;; `:sen-setting`) and `false` (`:sen-unit-indicator` &
;; `:resourced-provision-indicator`), allowing for more succinct
;; examples:

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-1)

;;; #### SEN settings
;; SENsettings `:sen-setting` are also mapped to `estab-cat` via the `estab-type-to-estab-cat` mapping:
^{::clerk/viewer clerk/table}
(def estab-type-to-estab-cat-ds-2
  (tc/concat-copying estab-type-to-estab-cat-ds-1
                     (tc/dataset [{:type-of-establishment-name    nil
                                   :sen-unit-indicator            false
                                   :resourced-provision-indicator false
                                   :sen-setting                   "OLA"
                                   :estab-cat                     "OLAS"}])))

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:sen-setting "OLA"}
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-2)


;;; ### Designations
;; To include designations in the settings:
;; 1. Specify which `estab-cat`s are to be designated.
;; 2. Specify a function to derive the designations.
;;
;; #1 is achieved via estab-cat attributes for each `estab-cat`, again
;; specified in the `::settings/estab-cats` config key; as a map,
;; dataset or via CSV filepath.
;;
;; …with the `:designate?` column/key set to `true` for the `estab-cat` `abbreviation`s to designate.
;; 
;; #2 is specified in `::settings/designation-f` key of the `cfg` map.
;; This should be a function that returns the designation abbreviation
;; given a map including the `sen2-estab` keys, `:estab-cat` (the
;; establishment category) and `:sen-provision-types-vec` (a vector of
;; the SEN provision type abbreviations for the establishment looked
;; up from GIAS). Function `settings/standard-designation`
;; implements the MC standard designations.

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-2
 ::settings/estab-cats              {"SpMdA" {:designate? true}}
 ::settings/designation-f           settings/standard-designation)


;;; ### Splitting by Area
;; To further split in vs. out or area into separate settings:
;; 1. Specify which `estab-cat`s are to be split.
;; 2. Specify a function to derive the area abbreviations.
;;
;; #1 is again achieved via estab-cat attributes, using the `:split-area?` column/key.

;; #2 is specified in the `::settings/area-split-f` key of the `cfg` map.
;; This should be a function that returns the area abbreviation
;; given a map including the `sen2-estab` keys, `:estab-cat`, 
;; :la-code (the `la-code` for the `:urn` from GIAS) and
;; `::settings/in-area-la-codes` (from the `cfg` map).  Function
;; `settings/standard-area-split` implements a simple area-split
;; returning "InA" for `:la-code`s in the set
;; `::settings/in-area-la-codes` and "OoA" for other (non-nil)
;; `:la-codes`.

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-2
 ::settings/estab-cats              {"SpMdA" {:designate?  true
                                              :split-area? true}}
 ::settings/designation-f           settings/standard-designation
 ::settings/area-split-f            settings/standard-area-split
 ::settings/in-area-la-codes        #{"879"})



;;; ## Manual settings
;; If a `sen2-estab` is not in GIAS (e.g. URN not in GIAS or specified
;; by UKPRN) or the resulting `estab-type`
;; {`:type-of-establishment-name` `:sen-unit-indicator`
;; `:resourced-provision-indicator` `:sen-setting`} is not covered by
;; the `estab-type-to-estab-cat` mapping (e.g. a GIAS "Welsh
;; establishment" or an unexpected `:sen-setting`), then the `setting`
;; is returned as "XxX":

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "401923"} ; In GIAS but of type "Welsh establishment".
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-2)

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:ukprn "10088118"} ; UKPRN: not in GIAS.
 ::settings/edubaseall-send-map     edubaseall-send-map
 ::settings/estab-type-to-estab-cat estab-type-to-estab-cat-ds-2)

;; For these establishments, the `estab-name`, `estab-cat`,
;; `designation` and `la-code` (from which any `area` split is
;; determined) must be specified manually, via map specified in the
;; `::settings/sen2-estab-settings-manual` config key; as a map, a
;; dataset or via CSV filepath.

;; For example, for the two cases above:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(def sen2-estab-settings-manual-ds-1
  (tc/dataset [{:urn                           "401923"
                :ukprn                         nil
                :sen-unit-indicator            false
                :resourced-provision-indicator false
                :sen-setting                   nil
                :estab-name                    "Greenfield Special School"
                :estab-cat                     "SpMdA"
	        :designation                   "SEMH"
	        :la-code                       "675"}
               {:urn                           nil
                :ukprn                         "10088118"
                :sen-unit-indicator            false
                :resourced-provision-indicator false
                :sen-setting                   nil
                :estab-name                    "Orchard Manor School"
                :estab-cat                     "SpMdA"
                :designation                   "HCOIN+SEMH"
                :la-code                       "878"}]))

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "401923"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-manual sen2-estab-settings-manual-ds-1)

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:ukprn "10088118"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-manual sen2-estab-settings-manual-ds-1)

;; The manual dataset/map is keyed by the `sen2-estab` columns/map, so
;; the dataset/map must contain columns/keys [`:urn` `:ukprn`
;; `:sen-unit-indicator` `:resourced-provision-indicator`
;; `:sen-setting`].

;; The value columns/keys [`:estab-name`, `:estab-cat` `:designation`
;; `:la-code`] are optional and can be omitted if not needed to hold
;; non-nil values.

;; Note that the manual setting components (`estab-name`, `estab-cat`,
;; `designation` and `la-code`) are _only_ used if they cannot be
;; determined via GIAS `esstab-type` lookup:
;; For example, for URN "113644" we can get `estab-type` via GIAS:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2)
;; …such that a manual setting entry has no effect:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-manual
 (tc/dataset [{:urn                           "113644"
               :ukprn                         nil
               :sen-unit-indicator            false
               :resourced-provision-indicator false
               :sen-setting                   nil
               :estab-cat                     "MANUAL~SETTING"}]))



;;; ## Override settings
;; There is also provision for "override" setting components:

;; These are specified like the manual settings but are applied in
;; preference to information obtained via GIAS or from the manual
;; settings.

;; For example (contrast with the final example in the "manual" section above):
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-override
 (tc/dataset [{:urn                           "113644"
               :ukprn                         nil
               :sen-unit-indicator            false
               :resourced-provision-indicator false
               :sen-setting                   nil
               :estab-cat                     "OVERRIDE~SETTING"}]))

;; Note that only non-nil components are used as overrides.
;; For example, an override record containing a non-nil `designation`
;; only may be used to over-ride an algorithmically determined
;; designation whilst retaining the `estab-cat` and `area` components:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate?  true
                                                 :split-area? true}}
 ::settings/designation-f              settings/standard-designation
 ::settings/area-split-f               settings/standard-area-split
 ::settings/in-area-la-codes           #{"879"}
 ::settings/sen2-estab-settings-override
 (tc/dataset [{:urn                           "113644"
               :ukprn                         nil
               :sen-unit-indicator            false
               :resourced-provision-indicator false
               :sen-setting                   nil
               :estab-cat                     nil
               :designation                   "OVERRIDE~DESIGNATION"
               :la-code                       nil}]))

;; Note that `designation`s and `area`s are only applied to
;; `estab-cat`s specified to be designated, even if an override (or
;; manual) `designation` or `la-code` is specified:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/edubaseall-send-map        edubaseall-send-map
 ::settings/estab-type-to-estab-cat    estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate?  false
                                                 :split-area? true}}
 ::settings/designation-f              settings/standard-designation
 ::settings/area-split-f               settings/standard-area-split
 ::settings/in-area-la-codes           #{"879"}
 ::settings/sen2-estab-settings-override
 (tc/dataset [{:urn                           "113644"
               :ukprn                         nil
               :sen-unit-indicator            false
               :resourced-provision-indicator false
               :sen-setting                   nil
               :estab-cat                     nil
               :designation                   "OVERRIDE~DESIGNATION"
               :la-code                       nil}]))



;;; ## Unknown settings
;; If the `sen2-estab` is unknown (i.e. all keys falsey), the setting is returned as "UKN":
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn                           nil
  :ukprn                         nil
  :sen-unit-indicator            false
  :resourced-provision-indicator false
  :sen-setting                   nil})



;;; ## (Ab)Use of `sen-setting`
;; For placement level specification of specific settings.
;; TODO



;;; ## Configuration from files
;; TODO
;; Note filepaths starting "resources/" are regarded as resource files.
;; Best `parse-cfg` to read in files rather than use a cfg specifying files, since in the latter case the files will be read on each invocation.


;;; ### MC Standard Settings
;; The "standard/" resources folder of `witan.send.settings` contain CSV configuration files for MC standard settings:


;;; ## Some scenarios
;;; ### Getting multiple values back
;; TODO
;; `:setting` and `:estab-name`


;;; ### Standard `estab-cats` but manual `designation`s
;; TODO


;;; ### Over-ride some of the derived designations
;; TODO


;;; ### Area to indicate LA internal vs. external specialist provision
;; TODO


;;; ### Using the `setting-split-regexp` regexp
;; Function `setting-split-regexp` returns a regexp for splitting setting abbreviations.
;;
;; Because some `estab-cat`s may be split but not designated and others designated but not split, we need to:
;; 1. Tell the function what the `area` abbreviations are (so it can greedily pull them off first).
;; 2. Ensure no overlap between `area` abbreviations.
;; TODO


;;; ### Settings used
;; TODO


;;; ### Specifying which edubaseall to use.
;; TODO



^{::clerk/visibility {:code :hide, :result :hide}}
(comment ;; clerk build
  (let [in-path            (str "notebooks/" (clojure.string/replace (str *ns*) #"\.|-" {"." "/" "-" "_"}) ".clj")
        out-path           (str (.getParentFile (io/file in-path)) "/" (clojure.string/replace (str *ns*) #"^.*\." "") ".html")]
    (clerk/build! {:paths    [in-path]
                   :ssr      true
                   :bundle   true
                   :out-path "."})
    (.renameTo (io/file "./index.html") (io/file out-path)))
  )
