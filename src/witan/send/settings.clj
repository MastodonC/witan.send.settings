(ns witan.send.settings
  "Define and assign tri-part establishment settings for witan SEND modelling."
  (:require [clojure.java.io :as io]
            [clojure.string  :as str]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]
            [witan.gias :as gias]))


;;; # Utility functions
(defn resource-or-file->dataset
  "Reads file at `filepath` into dataset using `ds/->dataset` with `options`.
   Paths beginning \"resources/\" are interpreted as resources."
  [filepath options]
  (with-open [in (-> (if (re-find #"^resources/" filepath)
                       (io/resource (str/replace filepath #"^resources/" "" ))
                       filepath)
                     io/file
                     io/input-stream)]
    (ds/->dataset in (merge {:dataset-name filepath} options))))

(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

(defn ds->hash-map
  "Given dataset `ds`, returns hash-map with
   - keys from the `key-cols` of `ds`.
   - vals from the remaining columns of `ds`,
     or just the `val-cols` if specified,
     as maps keyed by column name.
  If `key-cols` specifies multiple columns,
  then the keys are also returned as maps keyed by column name."
  [ds key-cols & {:keys [val-cols]}]
  (let [columns (fn [ds] (if (< 1 (tc/column-count ds))
                           (tc/rows ds :as-maps)
                           (-> (tc/columns ds :as-seqs) first)))
        ks       (-> ds
                     (tc/select-columns key-cols)
                     (columns))
        vs       (-> ds
                     ((fn [ds] (if val-cols
                                 (tc/select-columns ds val-cols)
                                 (tc/drop-columns ds key-cols))))
                     (tc/rows :as-maps))]
    (zipmap ks vs)))

(defn ds->sorted-map-by
  "Given dataset `ds`, returns sorted-map with
   - keys from the `key-col` of `ds` (values of which must compare).
   - vals from the remaining columns,
     or from just the `val-cols` if specified,
     as maps keyed by column name.
   - ordered by the order of rows in the `ds`,
     or by the values of `:order-col` if specified (which must compare)."
  [ds key-col & {:keys [val-cols order-col]}]
  (let [columns (fn [ds] (if (< 1 (tc/column-count ds))
                           (tc/rows ds :as-maps)
                           (-> (tc/columns ds :as-seqs) first)))
        ks       (-> ds
                     (tc/select-columns key-col)
                     (columns))
        vs       (-> ds
                     ((fn [ds] (if val-cols
                                 (tc/select-columns ds val-cols)
                                 (tc/drop-columns ds key-col))))
                     (tc/rows :as-maps))
        os       (or
                  (get ds order-col)
                  (range))]
    (into (sorted-map-by (partial compare-mapped-keys (zipmap ks os)))
          (zipmap ks vs))))

(defn map->ds
  "Given map `m`, returns dataset with keys and vals as columns.
   Column names via keyword parameters `:keys-col-name` & `:vals-col-name`.
   Keys or vals that are maps are expanded into columns named by the map keys."
  [m & {:keys [keys-col-name
               vals-col-name]
        :or   {keys-col-name :keys
               vals-col-name :vals}}]
  (->> m
       (reduce-kv (fn [vec-of-rows-as-maps k v]
                    (conj vec-of-rows-as-maps
                          (merge (if (map? k) k {keys-col-name k})
                                 (if (map? v) v {vals-col-name v}))))
                  [])
       tc/dataset))



;;; # Setting Definitions
(def base-def-csv-read-options
  "Base options for `ds/->dataset` read of setting definition CSV files."
  {:file-type        :csv
   :separator        ","
   :header-row?      :true
   :key-fn           keyword
   :parser-fn        {:abbreviation :string
                      :order        :int16
                      :name         :string
                      :label        :string
                      :definition   :string}})

(def base-def-ds-col-names
  "Base column names for definitions dataset."
  ((comp keys :parser-fn) base-def-csv-read-options))


;;; ## `:estab-cat`: Establishment Categories
(def estab-cats-csv-read-options
  "Options for `ds/->dataset` read of estab-cat definition CSV files."
  (merge-with merge
              base-def-csv-read-options
              {:parser-fn {:designate?  :boolean
                           :split-area? :boolean}}))

(def estab-cats-ds-col-names
  "Column names for estab-cat definitions dataset."
  ((comp keys :parser-fn) estab-cats-csv-read-options))

(defn estab-cats-ds
  "Return `estab-cats` dataset extracted/derived from `::estab-cats` key as follows:
   - value a map: dataset reconstructed from map
   - value a dataset: returned as-is
   - value a string: file at that path read as dataset"
  [& {::keys [estab-cats]}]
  (let [ds-template (tc/dataset nil {:dataset-name "estab-cats"
                                     :column-names estab-cats-ds-col-names})]
    (cond
      (string? estab-cats)     (resource-or-file->dataset estab-cats estab-cats-csv-read-options)
      (tc/dataset? estab-cats) estab-cats
      (map? estab-cats)        (tc/concat-copying ds-template (map->ds estab-cats :keys-col-name :abbreviation))
      :else                    ds-template)))

(defn estab-cats
  "Return `estab-cats` map extracted/derived from `::estab-cats` key as follows:
   - value a map: map returned as-is
   - value a dataset: made into a map keyed on `:abbreviation`
   - value a string: file at that path read as dataset and made into a map keyed on `:abbreviation`"
  [& {::keys [estab-cats] :as cfg}]
  (as-> estab-cats $
    (if (string? $)     (estab-cats-ds cfg) $)
    (if (tc/dataset? $) (ds->sorted-map-by $ :abbreviation {:order-col :order}) $)
    (if (map? $) $ {})))


;;; ## `:designation`: Designations
(defn designations-ds
  "Return `designations` dataset extracted/derived from `::designations` key as follows:
   - value a map: dataset reconstructed from map
   - value a dataset: returned as-is
   - value a string: file at that path read as dataset"
  [& {::keys [designations]}]
  (let [ds-template (tc/dataset nil {:dataset-name "designations"
                                     :column-names base-def-ds-col-names})]
    (cond
      (string? designations)     (resource-or-file->dataset designations base-def-csv-read-options)
      (tc/dataset? designations) designations
      (map? designations)        (tc/concat-copying ds-template (map->ds designations :keys-col-name :abbreviation))
      :else                      ds-template)))

(defn designations
  "Return `designations` map extracted/derived from `::designations` key as follows:
   - value a map: map returned as-is
   - value a dataset: made into a map keyed on `:abbreviation`
   - value a string: file at that path read as dataset and made into a map keyed on `:abbreviation`"
  [& {::keys [designations]}]
  (as-> designations $
    (if (string? $)     (resource-or-file->dataset $ base-def-csv-read-options) $)
    (if (tc/dataset? $) (ds->sorted-map-by $ :abbreviation {:order-col :order}) $)
    (if (map? $) $ {})))


;;; ## `:area`: Area
(defn areas-ds
  "Return `areas` dataset extracted/derived from `::areas` key as follows:
   - value a map: dataset reconstructed from map
   - value a dataset: returned as-is
   - value a string: file at that path read as dataset"
  [& {::keys [areas]}]
  (let [ds-template (tc/dataset nil {:dataset-name "areas"
                                     :column-names base-def-ds-col-names})]
    (cond
      (string? areas)     (resource-or-file->dataset areas base-def-csv-read-options)
      (tc/dataset? areas) areas
      (map? areas)        (tc/concat-copying ds-template (map->ds areas :keys-col-name :abbreviation))
      :else               ds-template)))

(defn areas
  "Return `areas` map extracted/derived from `::areas` key as follows:
   - value a map: map returned as-is
   - value a dataset: made into a map keyed on `:abbreviation`
   - value a string: file at that path read as dataset and made into a map keyed on `:abbreviation`"
  [& {::keys [areas]}]
  (as-> areas $
    (if (string? $)     (resource-or-file->dataset $ base-def-csv-read-options) $)
    (if (tc/dataset? $) (ds->sorted-map-by $ :abbreviation {:order-col :order}) $)
    (if (map? $) $ {})))


;;; ## Settings
;; Combining `:estab-cat`, `:designation` and `:area`:
(def settings-ds-col-name-prefixes
  "Column name prefixes for settings dataset."
  [nil "estab-cat" "designation" "area"])

(def settings-ds-col-names
  "Column names for settings dataset."
  (for [prefix           settings-ds-col-name-prefixes
        base-ds-col-name base-def-ds-col-names]
    (->> base-ds-col-name
         name
         (str prefix (when prefix "-"))
         keyword)))

(def settings-csv-read-options
  "Options for `ds/->dataset` read of setting definition CSV files."
  (update-in base-def-csv-read-options
             [:parser-fn]
             (fn [parser-fn-map col-name-prefixes]
               (reduce (fn [m col-name-prefix]
                         (merge m
                                (update-keys parser-fn-map
                                             (comp keyword
                                                   (partial str col-name-prefix (when col-name-prefix "-"))
                                                   name))))
                       {}
                       col-name-prefixes))
             settings-ds-col-name-prefixes))

(defn cfg->settings-ds
  "Derive dataset of setting abbreviations and attributes from:
   - estab-cat   definitions from `::estab-cats`
   - designation definitions from `::designations`
   - area        definitions from `::areas`."
  [& {estab-cats'   ::estab-cats
      designations' ::designations
      areas'        ::areas}]
  (let [prepend-str-to-keyword-f #(comp keyword (partial str % "-") name)
        estab-cats-ds'           (estab-cats-ds ::estab-cats estab-cats')
        designations-ds'         (designations-ds ::designations designations')
        areas-ds'                (areas-ds ::areas areas')]
    ;; FIXME: Drops estab-cats if should be designated|split-area but no designations|areas (which is a config error).
    ;; TODO: Only construct name|label|definition if have all components needed (i.e. need area if :estab-cat-split-area).
    (-> estab-cats-ds'
        (tc/rename-columns (prepend-str-to-keyword-f "estab-cat"))
        ;; Expand `:estab-cat`s to be designated by designations
        (as-> $
            (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-designate?))
                               (-> (tc/select-rows $ (comp :estab-cat-designate?))
                                   (tc/cross-join (-> designations-ds'
                                                      (tc/rename-columns (prepend-str-to-keyword-f "designation")))))))
        ;; Expand out `:estab-cat`s to be split by area
        (as-> $
            (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-split-area?))
                               (-> (tc/select-rows $ (comp :estab-cat-split-area?))
                                   (tc/cross-join (-> areas-ds'
                                                      (tc/rename-columns (prepend-str-to-keyword-f "area")))))))
        ;; Derive setting `:abbreviation`
        (tc/map-columns :abbreviation [:estab-cat-abbreviation :designation-abbreviation :area-abbreviation]
                        (fn [& args] (str/join "_" (filter some? args))))
        (tc/order-by [:estab-cat-order :designation-order :area-order])
        (tc/add-column :order (iterate inc 1))
        ;; Derive setting attributes from corresponding estab-cat, designation & area attributes (if available)
        (tc/map-rows (fn [{:keys [estab-cat-name       designation-name       area-name
                                  estab-cat-label      designation-label      area-label
                                  estab-cat-definition designation-definition area-definition]}]
                       (merge (when estab-cat-name
                                {:name (str/join " " (filter some? [area-name
                                                                    estab-cat-name
                                                                    designation-name]))})
                              (when estab-cat-label
                                {:label (str estab-cat-label
                                             (when area-label
                                               (format " (%s)" area-label))
                                             (when designation-label
                                               (format " - %s" designation-label)))})
                              (when estab-cat-definition
                                {:definition (str (when area-definition
                                                    (-> area-definition
                                                        (str/replace #".$" "")
                                                        (str " ")))
                                                  (-> estab-cat-definition
                                                      (str/replace #".$" ""))
                                                  (if designation-definition
                                                    (format
                                                     "; providing for (designation group) %s"
                                                     designation-definition)
                                                    "."))}))))
        ;; Tidy dataset
        (tc/select-columns settings-ds-col-names)
        (tc/set-dataset-name "settings"))))

(defn settings-ds
  "Return `settings` dataset extracted/derived from `::estab-cats`, `::designations` & `::areas`
   unless specified in truthy `::settings` key as follows:
   - value a map: dataset reconstructed from map
   - value a dataset: returned as-is
   - value a string: file at that path read as dataset"
  [& {::keys [settings] :as cfg}]
  (let [ds-template (tc/dataset nil {:dataset-name "settings"
                                     :column-names settings-ds-col-names})]
    (cond
      (nil? settings)        (cfg->settings-ds cfg)
      (string? settings)     (resource-or-file->dataset settings settings-csv-read-options)
      (tc/dataset? settings) settings
      (map? settings)        (tc/concat-copying ds-template (map->ds settings :keys-col-name :abbreviation))
      :else                   ds-template)))

(defn settings
  "Return `settings` map extracted/derived from `::estab-cats`, `::designations` & `::areas`
   unless specified in truthy `::settings` key as follows:
   - value a map: map returned as-is
   - value a dataset: made into a map keyed on `:abbreviation`
   - value a string: file at that path read as dataset and made into a map keyed on `:abbreviation`"
  [& {::keys [settings] :as cfg}]
  (as-> settings $
    (if (nil? $)        (cfg->settings-ds cfg) $)
    (if (string? $)     (settings-ds cfg) $)
    (if (tc/dataset? $) (ds->sorted-map-by $ :abbreviation {:order-col :order}) $)
    (if (map? $)        $ {})))

(defn settings-csv->settings-cfg
  "Read `settings` dataset from CSV file, extract `estab-cats`, `designations` and `areas` datasets,
   and returns in namespaced map."
  [settings-csv-filepath]
  (let [rename-component-columns  (fn [ds component]
                                    (-> ds
                                        (tc/rename-columns #(-> %
                                                                name
                                                                (str/replace (re-pattern (str "^" component "-")) "")
                                                                keyword))))
        settings-ds->component-ds (fn [ds component]
                                    (-> ds
                                        (tc/select-columns (re-pattern (str ":" component "-.+$")))
                                        (rename-component-columns component)
                                        (tc/drop-missing [:abbreviation])
                                        (tc/unique-by [:abbreviation] {:strategy :first})
                                        (tc/set-dataset-name (str component "s"))))
        settings-ds               (-> settings-csv-filepath
                                      (resource-or-file->dataset settings-csv-read-options))
        estab-cat-splits          (-> settings-ds
                                      (tc/group-by [:estab-cat-abbreviation])
                                      (tc/aggregate {:designate?  #(->> % :designation-abbreviation (not-every? nil?))
                                                     :split-area? #(->> % :area-abbreviation        (not-every? nil?))})
                                      (rename-component-columns "estab-cat"))
        estab-cats-ds             (-> settings-ds
                                      (settings-ds->component-ds "estab-cat")
                                      (tc/left-join estab-cat-splits [:abbreviation])
                                      (tc/drop-columns [:right.abbreviation])
                                      (tc/set-dataset-name "estab-cats"))
        designations-ds           (-> settings-ds
                                      (settings-ds->component-ds "designation"))
        areas-ds                  (-> settings-ds
                                      (settings-ds->component-ds "area"))]
    {::settings     settings-ds
     ::estab-cats   estab-cats-ds
     ::designations designations-ds
     ::areas        areas-ds}))

(defn setting-components-regex
  "Return regex pattern for splitting setting abbreviations into components.
   Relies on area abbreviations not clashing with designation abbreviations.
   Collection of areas can be specified (first supplied is used):
   - directly via `:area-abbreviations` key
   - via ::areas defined in settings cfg (via `(areas cfg)`)."
  [& {setting-components-regex' ::setting-components-regex
      area-abbreviations        :area-abbreviations ; Note deliberately NOT namespaced
      :as                       cfg}]
  (or setting-components-regex'
      (let [area-abbreviations' (or area-abbreviations
                                    (keys (areas cfg)))]
        (re-pattern (str "^(?<estabCat>[^_]+)"
                         "_??(?<designation>[^_]+)??"
                         "_?(?<area>" (str/join "|" area-abbreviations') ")?$")))))

(defn setting->components
  "Given `setting` abbreviation and `setting-components-regex`, returns map of setting components."
  [setting setting-components-regex]
  (let [[setting estab-cat designation area]
        (re-find setting-components-regex setting)]
    {:setting     setting
     :estab-cat   estab-cat
     :designation designation
     :area        area}))

(defn setting-components
  "Returns map of setting components extracted from `setting` abbreviation."
  [setting & {:as cfg}]
  (let [[setting estab-cat designation area]
        (re-find (setting-components-regex cfg) setting)]
    {:setting     setting
     :estab-cat   estab-cat
     :designation designation
     :area        area}))

(defn designation-component-regex
  "Return regex pattern for extracting designation component.
   Relies on area abbreviations not clashing with designation abbreviations.
   Collection of areas can be specified (first supplied is used):
   - directly via `:area-abbreviations` key
   - via ::areas defined in settings cfg (via `(areas cfg)`)."
  [& {designation-components-regex' ::designation-components-regex
      area-abbreviations            :area-abbreviations ; Note deliberately NOT namespaced
      :as                           cfg}]
  (or designation-components-regex'
      (let [area-abbreviations' (or area-abbreviations
                                    (keys (areas cfg)))]
        (re-pattern (str "(?<=_)"                                        ; positive lookbehind for an underscore
                         "(?!(" (str/join "|" area-abbreviations') ")$)" ; negative lookahead for area-abbreviations
                         "([^_]+)"                                       ; one or more chars except underscores
                         )))))



;;; # Setting mappings
(def sen2-estab-keys
  "SEN2 establishment column keywords from `placement-detail` table"
  [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting])


;;; ## Manual and Override settings
(def sen2-estab-settings-csv-read-options
  "Options for `ds/->dataset` read of sen2-estab setting CSV files."
  {:file-type        :csv
   :separator        ","
   :header-row?      :true
   :column-blocklist ["reference-website"
                      "notes"]
   :key-fn           keyword
   :parser-fn        {:urn                           :string
                      :ukprn                         :string
	              :sen-unit-indicator            :boolean
	              :resourced-provision-indicator :boolean
	              :sen-setting                   :string
	              :estab-name                    :string
	              :estab-cat                     :string
                      :designation                   :string
                      :la-code                       :string}})

(def sen2-estab-settings-ds-col-names
  "Column names for sen2-estab setting dataset."
  ((comp keys :parser-fn) sen2-estab-settings-csv-read-options))

(defn sen2-estab-settings-manual-ds
  "Dataset mapping SEN2 Estab keys to manual settings.
   Extracted/derived from `::sen2-estab-settings-manual` key.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-manual]}]
  (let [ds-template (tc/dataset nil {:dataset-name "sen2-estab-settings-manual"
                                     :column-names sen2-estab-settings-ds-col-names})]
    (cond
      (string? sen2-estab-settings-manual)     (resource-or-file->dataset sen2-estab-settings-manual sen2-estab-settings-csv-read-options)
      (tc/dataset? sen2-estab-settings-manual) sen2-estab-settings-manual
      (map? sen2-estab-settings-manual)        (tc/concat-copying ds-template (map->ds sen2-estab-settings-manual))
      :else                                    ds-template)))

(defn sen2-estab-settings-manual
  "Map SEN2 Estab keys to manual settings.
   Extracted/derived from `::sen2-estab-settings-manual` key.
   (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-manual]}]
  (as-> sen2-estab-settings-manual $
    (if (string? $)     (resource-or-file->dataset $ sen2-estab-settings-csv-read-options) $)
    (if (tc/dataset? $) (ds->hash-map $ sen2-estab-keys) $)
    (if (map? $) $ {})))

(defn sen2-estab-settings-override-ds
  "Dataset mapping SEN2 Estab keys to override settings.
   Extracted/derived from `::sen2-estab-settings-override` key.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-override]}]
  (let [ds-template (tc/dataset nil {:dataset-name "sen2-estab-settings-override"
                                     :column-names sen2-estab-settings-ds-col-names})]
    (cond
      (string? sen2-estab-settings-override)     (resource-or-file->dataset sen2-estab-settings-override sen2-estab-settings-csv-read-options)
      (tc/dataset? sen2-estab-settings-override) sen2-estab-settings-override
      (map? sen2-estab-settings-override)        (tc/concat-copying ds-template (map->ds sen2-estab-settings-override))
      :else                                      ds-template)))

(defn sen2-estab-settings-override
  "Map SEN2 Estab keys to override settings.
   Extracted/derived from `::sen2-estab-settings-override` key.
   (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-override]}]
  (as-> sen2-estab-settings-override $
    (if (string? $)     (resource-or-file->dataset $ sen2-estab-settings-csv-read-options) $)
    (if (tc/dataset? $) (ds->hash-map $ sen2-estab-keys) $)
    (if (map? $) $ {})))


;;; ## GIAS Establishment Type to `:estab-cat`
(def estab-type-keys [:type-of-establishment-name
                      :further-education-type-name-applicable
                      :sen-unit-indicator
	              :resourced-provision-indicator
	              :sen-setting])

(def estab-type-to-estab-cat-csv-read-options
  "Options for `ds/->dataset` read of estab-type-to-estab-cat CSV files."
  {:file-type   :csv
   :separator   ","
   :header-row? :true
   :key-fn      keyword
   :parser-fn   {:type-of-establishment-name             :string
                 :further-education-type-name-applicable :string
                 :sen-unit-indicator                     :boolean
                 :resourced-provision-indicator          :boolean
                 :sen-setting                            :string
                 :estab-cat                              :string}})

(def estab-type-to-estab-cat-ds-col-names
  "Column names for estab-type-to-estab-cat dataset."
  ((comp keys :parser-fn) estab-type-to-estab-cat-csv-read-options))

(defn estab-type-to-estab-cat-ds
  "Dataset mapping Estab Types to `:estab-cat`s.
   Extracted/derived from `::estab-type-to-estab-cat` key.
   (Estab Type = GIAS Establishment Types split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [estab-type-to-estab-cat]}]
  (let [ds-template (tc/dataset nil {:dataset-name "estab-type-to-estab-cat"
                                     :column-names estab-type-to-estab-cat-ds-col-names})]
    (cond
      (string? estab-type-to-estab-cat)     (resource-or-file->dataset estab-type-to-estab-cat estab-type-to-estab-cat-csv-read-options)
      (tc/dataset? estab-type-to-estab-cat) estab-type-to-estab-cat
      (map? estab-type-to-estab-cat)        (tc/concat-copying ds-template (map->ds estab-type-to-estab-cat :vals-col-name :estab-cat))
      :else                                 ds-template)))

(defn estab-type-to-estab-cat
  "Map Estab Types to `:estab-cat`s.
   Extracted/derived from `::estab-type-to-estab-cat` key.
   (Estab Type = GIAS Establishment Types split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [estab-type-to-estab-cat]}]
  (as-> estab-type-to-estab-cat $
    (if (string? $)     (resource-or-file->dataset $ estab-type-to-estab-cat-csv-read-options) $)
    (if (tc/dataset? $) (-> $ (ds->hash-map estab-type-keys) (update-vals :estab-cat)) $)
    (if (map? $) $ {})))


;;; ## Designation derivation
(defn sen-provision-types-vec->designation
  "Returns MC standard designation given vector of SEN provision types extracted from GIAS."
  ;; | Area of Need                            | Short Code | Needs Served          |
  ;; |:----------------------------------------|:-----------|:----------------------|
  ;; | Social, Emotional & Mental Health Needs | SEMH       | SEMH                  |
  ;; | Communication & Interaction Needs       | COIN       | SLCN                  |
  ;; | High Communication & Interaction Needs  | HCOIN      | ASD                   |
  ;; | High COIN & SEMH                        | HCOIN+SEMH | ASD Plus SEMH         |
  ;; | Sensory & Physical Disability Needs     | SPN        | HI or VI or PD        |
  ;; | Cognition & Learning Needs              | C+L        | MLD or SpLD           |
  ;; | High Cognition & Learning Needs         | HC+L       | SLD or PMLD           |
  ;; | Complex Social & Care Needs             | CS+CN      | ASD, Plus SLD or PMLD |
  ;;
  ;; The dividing line between one broad area and another being whether or
  ;; not they will accept some needs. SEMH schools often won’t accept ASD
  ;; for example. At most of these establishments other needs will be
  ;; served as well.
  [v]
  (cond
    (and (some #{"ASD"}               v)
         (some #{"SLD" "PMLD"}        v)) "CS+CN"
    (and (some #{"ASD"}               v)
         (some #{"SEMH"}              v)) "HCOIN+SEMH"
    (some      #{"SEMH"}              v)  "SEMH"
    (some      #{"ASD"}               v)  "HCOIN"
    (some      #{"SLD" "PMLD"}        v)  "HC+L"
    (some      #{"SLCN"}              v)  "COIN"
    (some      #{"HI" "VI" "PD"}      v)  "SPN"
    (some      #{"MLD" "SPLD" "SpLD"} v)  "C+L"
    :else                           "GEN"))

(defn standard-designation
  [& {:keys [sen-provision-types-vec]}]
  (sen-provision-types-vec->designation sen-provision-types-vec))


;;; ## Area derivation
(defn area-split-for-la-code
  "Returns \"InA\" if `la-code` is in set `in-area-la-codes`, \"OoA\" if not and not nil, otherwise \"XxX\"."
  [in-area-la-codes la-code]
  (cond (in-area-la-codes la-code) "InA"
        (some? la-code)            "OoA"
        :else                      "XxX"))

(defn standard-area-split
  [& {:keys  [la-code]
      ::keys [in-area-la-codes]}]
  (area-split-for-la-code in-area-la-codes la-code))



;;; # Configs
(defn parse-cfg-files
  "Parse settings configuration map `cfg` replacing string filepath
   values of selected keys with the dataset read from the file."
  [& {:as cfg}]
  (let [parse-string-v (fn [k v f] (if (string? v) (f k v) v))]
    (reduce-kv
     (fn [m k v]
       (assoc m k (case k
                    ::estab-cats                   (parse-string-v k v estab-cats-ds)
                    ::designations                 (parse-string-v k v designations-ds)
                    ::areas                        (parse-string-v k v areas-ds)
                    ::settings                     (parse-string-v k v settings-ds)
                    ::sen2-estab-settings-manual   (parse-string-v k v sen2-estab-settings-manual-ds)
                    ::sen2-estab-settings-override (parse-string-v k v sen2-estab-settings-override-ds)
                    ::estab-type-to-estab-cat      (parse-string-v k v estab-type-to-estab-cat-ds)
                    v)))
     {}
     cfg)))

(defn parse-cfg
  "Parse settings configuration map `cfg` replacing string filepath
   values of selected keys with the dataset read from the file,
   and add ::edubaseall-send-map from GIAS if not already in `cfg`."
  [& {::keys [edubaseall-send-map]
      :as    cfg}]
  (merge
   (parse-cfg-files cfg)
   (when (nil? edubaseall-send-map) {::edubaseall-send-map (gias/edubaseall-send->map cfg)})))

(def standard-cfg-file-names
  "Configuration keys for standard settings CSV file names (without location)."
  {::estab-cats                   "estab-cats.csv"
   ::designations                 "designations.csv"
   ::areas                        "areas.csv"
   ::sen2-estab-settings-manual   "sen2-estab-settings-manual.csv"
   ::sen2-estab-settings-override "sen2-estab-settings-override.csv"
   ::estab-type-to-estab-cat      "estab-type-to-estab-cat.csv"})

(def standard-cfg-files
  "Configuration keys for standard settings CSV files."
  (merge (update-vals standard-cfg-file-names
                      (partial str "resources/standard/"))
         {::designation-f standard-designation
          ::area-split-f  standard-area-split}))

(defn standard-cfg
  "Standard settings config, including GIAS `::edubaseall-send-map`."
  [& {:as cfg}]
  (parse-cfg (merge standard-cfg-files cfg)))



;;; # Settings for sen2-estab
(defn sen2-estab->setting-map
  [{:keys [urn
           ukprn
           sen-unit-indicator
           resourced-provision-indicator
           sen-setting]
    :or   {urn                           nil
           ukprn                         nil
           sen-unit-indicator            false
           resourced-provision-indicator false
           sen-setting                   nil}}
   & {estab-cats'                        ::estab-cats                   ; required to specify :estab-cats to designate|area-split
      sen2-estab-settings-override'      ::sen2-estab-settings-override ; optional
      sen2-estab-settings-manual'        ::sen2-estab-settings-manual   ; optional
      estab-type-to-estab-cat'           ::estab-type-to-estab-cat      ; required for GIAS lookups
      ::keys [edubaseall-send-map                                       ; required for GIAS lookups
              designation-f
              area-split-f
              in-area-la-codes
              sen-unit-name
              resourced-provision-name]
      :or    {edubaseall-send-map      {}
              designation-f            (constantly nil)
              area-split-f             (constantly nil)
              in-area-la-codes         #{}
              sen-unit-name            "(SEN Unit)"
              resourced-provision-name "(Resourced Provision)"}
      :as    cfg}]
  (let [sen2-estab          {:urn                           urn
                             :ukprn                         ukprn
                             :sen-unit-indicator            sen-unit-indicator
                             :resourced-provision-indicator resourced-provision-indicator
                             :sen-setting                   sen-setting}
        ;; Get any override or manual setting information for this sen2-estab
        override            (get (sen2-estab-settings-override
                                  ::sen2-estab-settings-override
                                  sen2-estab-settings-override') sen2-estab)
        manual              (get (sen2-estab-settings-manual
                                  ::sen2-estab-settings-manual
                                  sen2-estab-settings-manual') sen2-estab)
        ;; Get GIAS information for this sen2-estab
        edubaseall-send     (cond
                              urn
                              (edubaseall-send-map urn)
                              ukprn
                              (some #(when (= ukprn (:ukprn %)) %) (vals edubaseall-send-map))
                              :else
                              nil)
        estab-name-via-gias (let [establishment-name (:establishment-name edubaseall-send)]
                              (when establishment-name
                                (str establishment-name
                                     (when sen-unit-indicator (str " " sen-unit-name))
                                     (when resourced-provision-indicator (str " " resourced-provision-name)))))
        ;; Derive estab-type from GIAS and sen2-estab info. Note this handles sen-setting too.
        estab-type          {:type-of-establishment-name             (get edubaseall-send :type-of-establishment-name)
                             :further-education-type-name-applicable (get edubaseall-send :further-education-type-name-applicable)
                             :sen-unit-indicator                     sen-unit-indicator
                             :resourced-provision-indicator          resourced-provision-indicator
                             :sen-setting                            sen-setting}
        ;; Lookup estab-cat for this estab-type
        estab-cat-via-gias  (get (estab-type-to-estab-cat
                                  ::estab-type-to-estab-cat
                                  estab-type-to-estab-cat') estab-type)
        ;; Derive estab-name: Precedence: override > gias (with SENU|RP indicated) > manual > (SEN setting)
        estab-name          (or (get override :estab-name)
                                estab-name-via-gias
                                (get manual :estab-name)
                                (when sen-setting (format "(SEN Setting: %s)" sen-setting)))
        ;; Derive estab-cat: Set to "XxX" if have truthy values for sen2-estab keys but can't derive
        estab-cat           (or (get override :estab-cat)
                                estab-cat-via-gias
                                (get manual :estab-cat)
                                (when (not-any? boolean (vals sen2-estab)) "UKN")
                                "XxX") ; Avoid "XxX" via manual or override.
        ;; Derive designation if estab-cat to be designated: Set to "XxX" if have truthy values for sen2-estab keys but can't derive
        designate?          (get-in (estab-cats ::estab-cats estab-cats') [estab-cat :designate?])
        designation         (when designate? (or (get override :designation)
                                                 (designation-f (assoc sen2-estab
                                                                       :estab-cat estab-cat
                                                                       :sen-provision-types-vec (get edubaseall-send :sen-provision-types-vec)
                                                                       :edubaseall-send edubaseall-send))
                                                 (get manual :designation)
                                                 (when (not-any? boolean (vals sen2-estab)) "UKN") ; Only possible if estab-cat "UKN" designated!?
                                                 "XxX")) ; Avoid "XxX" via manual, designation-f, or override.
        ;; Derive `:area` for `:estab-cat`s to be split by area:
        split-area?         (get-in (estab-cats ::estab-cats estab-cats') [estab-cat :split-area?])
        la-code             (or (get override :la-code)
                                (get edubaseall-send :la-code)
                                (get manual :la-code))
        area                (when split-area? (or (area-split-f (assoc sen2-estab
                                                                       :estab-cat estab-cat
                                                                       :la-code la-code
                                                                       ::in-area-la-codes in-area-la-codes))
                                                  (when (not-any? boolean (vals sen2-estab)) "UKN") ; Only possible if estab-cat "UKN" designated!?
                                                  "XxX")) ; Avoid "XxX" via manual, area-split-f, or override.
        ;; Derive setting from estab-cat, designation and area abbreviations
        setting             (str/join "_" (filter some? [estab-cat designation area]))]
    {:sen2-estab          sen2-estab
     :override            override
     :edubaseall-send     edubaseall-send
     :estab-name-via-gias estab-name-via-gias
     :estab-type          estab-type
     :estab-cat-via-gias  estab-cat-via-gias
     :manual              manual
     :estab-name          estab-name
     :estab-cat           estab-cat
     :designate?          designate?
     :designation         designation
     :split-area?         split-area?
     :la-code             la-code
     :area                area
     :setting             setting}))

(defn sen2-estab->setting
  [sen2-estab & {:as cfg}]
  (:setting (sen2-estab->setting-map sen2-estab cfg)))
