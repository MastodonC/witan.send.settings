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
;; up from GIAS). Function `settings/standard-designation-f`
;; implements the MC standard designations.

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate? true}}
 ::settings/designation-f              settings/standard-designation-f)


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
;; `settings/standard-area-split-f` implements a simple area-split
;; returning "InA" for `:la-code`s in the set
;; `::settings/in-area-la-codes` and "OoA" for other (non-nil)
;; `:la-codes`.

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "113644"}
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2
 ::settings/estab-cats                 {"SpMdA" {:designate?  true
                                                 :split-area? true}}
 ::settings/designation-f              settings/standard-designation-f
 ::settings/area-split-f               settings/standard-area-split-f
 ::settings/in-area-la-codes           #{"879"})


;;; ## Manual settings
;; If a `sen2-estab` is not in GIAS (e.g. URN not in GIAS or specified
;; by UKPRN) or the resulting `estab-type`
;; {`:type-of-establishment-name` `:sen-unit-indicator`
;; `:resourced-provision-indicator` `:sen-setting`} is not covered by
;; the `estab-type-to-estab-cat` ;; mapping (e.g. a GIAS "Welsh
;; establishment" or an unexpected `:sen-setting`), then the `setting`
;; is returned as "XxX":

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:urn "401923"} ; In GIAS but of type "Welsh establishment"
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2)

^{::clerk/viewer clerk/md}
(settings/sen2-estab->setting
 {:ukprn "10088118"} ; UKPRN: not in GIAS
 ::settings/estab-type-to-estab-cat-ds estab-type-to-estab-cat-ds-2)

;; For these establishments, the `estab-cat`, `designation` and
;; `la-code` (from which any `area` split is determined) must be
;; specified manually, via `sen2-estab-settings-manual` map, again
;; specified in the `cfg` map: - directly as
;; `::settings/sen2-estab-settings-manual`; - via dataset
;; `::settings/sen2-estab-settings-manual-ds`; - or via CSV file
;; specified by one or more of
;; `::settings/sen2-estab-settings-manual-filename`,
;; `::settings/resource-dir` or `::settings/dir`.

^{::clerk/viewer clerk/table}
(def sen2-estab-settings-manual-ds-1
  (tc/dataset [{:urn                           "401923"
                :ukprn                         nil
                :sen-unit-indicator            false
                :resourced-provision-indicator false
                :sen-setting                   nil
                :estab-cat                     "SpMdA"
	        :designation                   "SEMH"
	        :la-code                       "675"}
               {:urn                           nil
                :ukprn                         "10088118"
                :sen-unit-indicator            false
                :resourced-provision-indicator false
                :sen-setting                   nil
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

;; Note that the manual setting components are _only_ used if they cannot be determined via GIAS `esstab-type` lookup:
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
               :estab-cat                     "**MANUAL**"}]))


;;; ## Override settings
;; There is also provision for "override" settings: These are specified like the manual settings but 


;;; ## Unknown settings
;; If the `sen2-estab` is unknown (i.e. all keys falsey), the setting is returned as "UKN":


;;; ## (Ab)Use of `sen-setting`
;; For placement level spevification of specific settings.


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
