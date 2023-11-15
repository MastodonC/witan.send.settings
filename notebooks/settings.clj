(ns settings
  "Clerk notebook illustrating use of `witan.send.settings`"
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.java.io :as io]
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
;;; ### Basic use with standard MC settings
;; To get the setting for a sen2-estab
;; {`:urn` `:ukprn` `:sen-unit-indicator` `:resourced-provision-indicator` `:sen-setting`}
;; using the MC standard settings from the library resource folder,
;; with LA funded establishment categories split into in vs. out of
;; area for local LA code "879":
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn                           "113644"
  :ukprn                         nil
  :sen-unit-indicator            false
  :resourced-provision-indicator false
  :sen-setting                   nil}
 ::settings/resource-dir     "standard/"
 ::settings/designation-f    settings/standard-designation
 ::settings/area-split-f     settings/standard-area-split
 ::settings/in-area-la-codes #{"879"})

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

;;; ### Other functionality
;; - To get estab-name, setting components and intermediate values used
;;   in the derivation of the setting (including results of the GIAS
;;   lookup), use `settings/sen2-estab->setting-map`.
;; - `settings/setting-split-regexp` returns a regex that will extract
;;   the `estab-cat`, `designation` and `area` abbreviations from a
;;   `setting` abbreviation.
;; - Definitions of the `etab-cat`, `designation` and `area` components
;;   of the settings (including labels and orders) as well as the
;;   resulting set of (all possible) `setting`s are available via
;;   functions `settings/estab-cats`, `settings/designations`,
;;   `settings/areas` and `settings/settings`.
;; - Configuration is via `cfg` map argument (or trailing key-value
;;   pairs)
;; - Lookups (e.g. for setting component definitions or mappings) are
;;   used as maps but can be specified as datasets or CSV files or
;;   directory (resource or path), with the first given used.
;;   - This allows customisation, for example by using the standard
;;     `::settings/resource-dir "standard/"` settings and mappings
;;     overall but specifying a custom set of
;;     `sen2-estab-settings-manual` settings via
;;     `::settings/sen2-estab-settings-manual-filename "./sen2-estab-settings-manual.csv"`.
;; - Functions that derive the lookup maps (from datasets - e.g. `settings/estab-cats`) or lookup
;;   datasets (from CSV files) have options to specify the desired map
;;   or dataset.
;;   - This allows customisation: For example tweaking the standard
;;     `estab-cats` map to turn off splitting of "SENU" by area.

;; For details, read on…


;;; ## Settings via GIAS
;; Given a `sen2-estab` (map) and settings `cfg` map, function
;; `sen2-estab->setting` returns the setting.

;;; ### `estab-cat`
;; SEN2 estabs are mapped to Establishment Category `:estab-cat` via GIAS
;; `type-of-establishment-name`, a `estab-type-to-estab-cat` mapping
;; **must** be specified in the settings `cfg.

;; This can be specified in the `cfg` map:
;; - directly as `::settings/estab-type-to-estab-cat`;
;; - via dataset `::settings/estab-type-to-estab-cat-ds`;
;; - or via CSV file specified by one or more of `::settings/estab-type-to-estab-cat-filename`, `::settings/resource-dir` or `::settings/dir`.

;;; #### URNs wtih SENU|RP indicators
;; Here's a simple example (as a dataset) for a single estab-type:
^{::clerk/viewer clerk/table}
(def estab-type-to-estab-cat-ds-1
  (tc/dataset [{:type-of-establishment-name    "Foundation special school"
                :sen-unit-indicator            false
                :resourced-provision-indicator false
                :sen-setting                   nil
                :estab-cat                     "SpMdA"}]))

;; which we can use to get the setting for a `sen2-estab` as follows:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn                           "113644"
  :ukprn                         nil
  :sen-unit-indicator            false
  :resourced-provision-indicator false
  :sen-setting                   nil}
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-1)

;; The `sen2-estab` map is destructured within `sen2-estab->setting` and missing/nil keys set to `nil` (`:urn`, `:ukprn` & `:sen-setting`) and `false` (`:sen-unit-indicator` & `:resourced-provision-indicator`), allowing for more succinct examples:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-1)

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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2)


;;; ### Designations
;; To include designations in the settings:
;; 1. Specify which `estab-cat`s are to be designated.
;; 2. Specify a function to derive the designations.
;;
;; #1 is acheived via estab-cat attributes for each `estab-cat`, again specified in the `cfg` map:
;; - directly as `::settings/estab-cats`;
;; - via dataset `::settings/estab-cats-ds`;
;; - or via CSV file specified by one or more of `::settings/estab-cats-filename`, `::settings/resource-dir` or `::settings/dir`.
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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate? true}}
 ::settings/designation-f              settings/standard-designation)


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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate?  true
                                                 :split-area? true}}
 ::settings/designation-f              settings/standard-designation
 ::settings/area-split-f               settings/standard-area-split
 ::settings/in-area-la-codes           #{"879"})


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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2)

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:ukprn "10088118"} ; UKPRN: not in GIAS.
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2)

;; For these establishments, the `estab-name`, `estab-cat`,
;; `designation` and `la-code` (from which any `area` split is
;; determined) must be specified manually, via
;; `sen2-estab-settings-manual` map, again specified in the `cfg` map:
;; - directly as `::settings/sen2-estab-settings-manual`
;; - via dataset `::settings/sen2-estab-settings-manual-ds`
;; - or via CSV file specified by one or more of
;;   `::settings/sen2-estab-settings-manual-filename`,
;;   `::settings/resource-dir` or `::settings/dir`.

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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-manual-ds sen2-estab-settings-manual-ds-1)

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:ukprn "10088118"}
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-manual-ds sen2-estab-settings-manual-ds-1)

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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2)
;; …such that a manual setting entry has no effect:
^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-manual-ds
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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/sen2-estab-settings-override-ds
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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate?  true
                                                 :split-area? true}}
 ::settings/designation-f              settings/standard-designation
 ::settings/area-split-f               settings/standard-area-split
 ::settings/in-area-la-codes           #{"879"}
 ::settings/sen2-estab-settings-override-ds
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
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate?  false
                                                 :split-area? true}}
 ::settings/designation-f              settings/standard-designation
 ::settings/area-split-f               settings/standard-area-split
 ::settings/in-area-la-codes           #{"879"}
 ::settings/sen2-estab-settings-override-ds
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


;;; ## Configuration from files
;; Recall configuration datasets/maps can be specified explicitly as
;; maps/datasets via the corresponding
;; `::settings/*`/`::settings/*-ds` keys as illustrated above, but
;; also from CSV files specified via `::settings/*-filename`,
;; `::settings/dir` & `::settings/resource-dir` keys.



;;; ## Some scenarios
;;; ### Standard `estab-cats` but manual `designation`s
;;; ### Over-ride some of the derived designations
;;; ### Area to indicate LA internal vs. external specialist provision


;;; ## Using CSV configs
;;; ### MC Standard Settings
;; The "standard/" resources folder of `witan.send.settings` contain CSV configuration files for MC standard settings:
;; - 


;;; ## GIAS tweaks
;;; ### Specifying which edubaseall to use.

;;; ### Speeding it up
;; The GIAS edubaseall dataset is huge, such that looking up URNs in a hash-map made from it is quite slow.
;;
;; To speed things up, create a edubaseall filtered for the URNs in your data and pass it to `` via `::settings/edubaseall-send-map`.


;;; # Setting handling
;;; ## Splitting setting abbreviations
;; Function `setting-split-regexp` returns a regexp for splitting setting abbreviations.
;;
;; Because some `estab-cat`s may be split but not designated and others designated but not split, we need to:
;; 1. Tell the function what the `area` abbreviations are (so it can greedily pull them off first).
;; 2. Ensure no overlap between `area` abbreviations.

