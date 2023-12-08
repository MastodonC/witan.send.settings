(ns witan.send.settings
  "Define and assign tri-part establishment settings for witan SEND modelling."
  (:require [clojure.java.io :as io]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]
            [witan.gias :as gias]))


;;; # Utility functions
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
(defn resource-or-file->dataset
  "Reads file at `filepath` into dataset using `ds/->dataset` with `options`.
   Paths beginning \"resources/\" are interpreted as resources."
  [filepath options]
  (let [in (-> (if (re-find #"^resources/" filepath)
                 (io/resource (string/replace filepath #"^resources/" "" ))
                 filepath)
               io/file
               io/input-stream)]
    (ds/->dataset in (merge {:dataset-name filepath} options))))

(def setting-defs-csv->ds-opts
  "Common options for `ds/->dataset` read of setting defs CSV files."
  {:file-type        :csv
   :separator        ","
   :header-row?      :true
   :key-fn           keyword
   :parser-fn        {:abbreviation :string
                      :order        :int16
                      :name         :string
                      :label        :string
                      :definition   :string}})


;;; ## `:estab-cat`: Establishment Categories
(defn estab-cats-ds
  "Return `estab-cats` dataset extracted/derived from `::estab-cats` key as follows:
   - value a map: dataset reconstructed from map
   - value a dataset: returned as-is
   - value a string: file at that path read as dataset"
  [& {::keys [estab-cats]}]
  (let [ds-template (tc/dataset nil {:dataset-name "estab-cats"
                                     :column-names (concat ((comp keys :parser-fn) setting-defs-csv->ds-opts)
                                                           [:designate? :split-area?])})]
    (cond
      (string? estab-cats)     (resource-or-file->dataset estab-cats (merge-with merge
                                                                                 setting-defs-csv->ds-opts
                                                                                 {:parser-fn {:designate?  :boolean
                                                                                              :split-area? :boolean}}))
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
  (let [ds-template (tc/dataset nil {:dataset-name "designation"
                                     :column-names ((comp keys :parser-fn) setting-defs-csv->ds-opts)})]
    (cond
      (string? designations)     (resource-or-file->dataset designations setting-defs-csv->ds-opts)
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
    (if (string? $)     (resource-or-file->dataset $ setting-defs-csv->ds-opts) $)
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
                                     :column-names ((comp keys :parser-fn) setting-defs-csv->ds-opts)})]
    (cond
      (string? areas)     (resource-or-file->dataset areas setting-defs-csv->ds-opts)
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
    (if (string? $)     (resource-or-file->dataset $ setting-defs-csv->ds-opts) $)
    (if (tc/dataset? $) (ds->sorted-map-by $ :abbreviation {:order-col :order}) $)
    (if (map? $) $ {})))


;;; ## Settings
;; Combining `:estab-cat`, `:designation` and `:area`:
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
    ;; FIXME: Drops estab-cats if should be designated|splie-area but no designations|areas (which is a config error).
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
                        (fn [& args] (clojure.string/join "_" (filter some? args))))
        (tc/order-by [:estab-cat-order :designation-order :area-order])
        (tc/add-column :order (iterate inc 1))
        ;; Derive setting attributes from corresponding estab-cat, designation & area attributes (if available)
        (tc/map-rows (fn [{:keys [estab-cat-name       designation-name       area-name
                                  estab-cat-label      designation-label      area-label
                                  estab-cat-definition designation-definition area-definition]}]
                       (let [setting-attr-f (fn [estab-cat-attr designation-attr area-attr]
                                              (str estab-cat-attr
                                                   (when designation-attr (format " - %s" designation-label))
                                                   (when area-attr (format " [%s]" area-label))))]
                         (merge (when estab-cat-name
                                  {:name (setting-attr-f estab-cat-name       designation-name       area-name      )})
                                (when estab-cat-label
                                  {:label (setting-attr-f estab-cat-label      designation-label      area-label     )})
                                (when estab-cat-definition
                                  {:definition (setting-attr-f estab-cat-definition designation-definition area-definition)})))))
        ;; Tidy dataset
        (tc/rename-columns {:estab-cat-abbreviation   :estab-cat
                            :designation-abbreviation :designation
                            :area-abbreviation        :area})
        (tc/select-columns [:abbreviation :order :name :label :definition :estab-cat :designation :area]))))

(defn settings-ds
  "Dataset of setting abbreviations and attributes.
   Derived from estab-cat, designation & area definitions unless specified in truthy `::settings` val."
  [& {settings' ::settings
      :as       cfg}]
  (let [settings-ds-cols [:abbreviation :order :name :label :definition :estab-cat :designation :area]
        ds-template      (tc/dataset nil {:dataset-name "settings"
                                          :column-names settings-ds-cols})]
    (cond
      (nil? settings')        (-> cfg
                                  cfg->settings-ds)
      (tc/dataset? settings') settings'
      (map? settings')        (tc/concat-copying ds-template
                                                 (map->ds settings' :keys-col-name :abbreviation))
      :else                   ds-template)))

(defn settings
  "Map setting abbreviations to attributes.
   Derived from estab-cat, designation & area definitions unless specified in truthy `::settings` val."
  [& {settings' ::settings
      :as       cfg}]
  (cond
    (nil? settings')        (-> cfg
                                (cfg->settings-ds)
                                (ds->sorted-map-by :abbreviation {:order-col :order}))
    (tc/dataset? settings') (-> settings'
                                (ds->sorted-map-by :abbreviation {:order-col :order}))
    (map? settings')        settings'
    :else                   {}))

(defn setting-split-regexp
  "Return regex pattern for splitting setting abbreviations into components.
   Relies on area abbreviations not clashing with designation abbreviations.
   Collection of areas can be specified (first supplied is used):
   - directly via `:area-abbreviations` key
   - via keys of areas map specified in `:areas` key
   or (if not) is via keys of areas map derived from settings cfg via `(areas cfg)`."
  [& {setting-split-regex' ::setting-split-regex
      area-abbreviations   :area-abbreviations ; Note deliberately NOT namespaced
      areas'               ::areas
      :as                  cfg}]
  (or setting-split-regex'
      (let [area-abbreviations' (or area-abbreviations
                                    (keys (or areas'
                                              (areas cfg))))]
        (re-pattern (str "^(?<estabCat>[^_]+)"
                         "_??(?<designation>[^_]+)??"
                         "_?(?<area>" (string/join "|" area-abbreviations') ")?$")))))



;;; # Setting mappings
(def sen2-estab-keys
  "SEN2 establishment column keywords from `placement-detail` table"
  [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting])


;;; ## Manual and Override settings
(def sen2-estab-settings-csv->ds-opts
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
	              :estab-cat                     :string
                      :designation                   :string
                      :la-code                       :string}})

(defn sen2-estab-settings-manual-ds
  "Dataset mapping SEN2 Estab keys to manual settings.
   Extracted/derived from `::sen2-estab-settings-manual` key.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-manual]}]
  (let [ds-template (tc/dataset nil {:dataset-name "sen2-estab-settings-manual"
                                     :column-names ((comp keys :parser-fn) sen2-estab-settings-csv->ds-opts)})]
    (cond
      (string? sen2-estab-settings-manual)     (resource-or-file->dataset sen2-estab-settings-manual sen2-estab-settings-csv->ds-opts)
      (tc/dataset? sen2-estab-settings-manual) sen2-estab-settings-manual
      (map? sen2-estab-settings-manual)        (tc/concat-copying ds-template (map->ds sen2-estab-settings-manual))
      :else                                    ds-template)))

(defn sen2-estab-settings-manual
  "Map SEN2 Estab keys to manual settings.
   Extracted/derived from `::sen2-estab-settings-manual` key.
   (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-manual]}]
  (as-> sen2-estab-settings-manual $
    (if (string? $)     (resource-or-file->dataset $ sen2-estab-settings-csv->ds-opts) $)
    (if (tc/dataset? $) (ds->hash-map $ sen2-estab-keys) $)
    (if (map? $) $ {})))

(defn sen2-estab-settings-override-ds
  "Dataset mapping SEN2 Estab keys to override settings.
   Extracted/derived from `::sen2-estab-settings-override` key.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-override]}]
  (let [ds-template (tc/dataset nil {:dataset-name "sen2-estab-settings-override"
                                     :column-names ((comp keys :parser-fn) sen2-estab-settings-csv->ds-opts)})]
    (cond
      (string? sen2-estab-settings-override)     (resource-or-file->dataset sen2-estab-settings-override sen2-estab-settings-csv->ds-opts)
      (tc/dataset? sen2-estab-settings-override) sen2-estab-settings-override
      (map? sen2-estab-settings-override)        (tc/concat-copying ds-template (map->ds sen2-estab-settings-override))
      :else                                      ds-template)))

(defn sen2-estab-settings-override
  "Map SEN2 Estab keys to override settings.
   Extracted/derived from `::sen2-estab-settings-override` key.
   (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [sen2-estab-settings-override]}]
  (as-> sen2-estab-settings-override $
    (if (string? $)     (resource-or-file->dataset $ sen2-estab-settings-csv->ds-opts) $)
    (if (tc/dataset? $) (ds->hash-map $ sen2-estab-keys) $)
    (if (map? $) $ {})))


;;; ## GIAS Establishment Type to `:estab-cat`
(def estab-type-keys [:type-of-establishment-name
                      :sen-unit-indicator
	              :resourced-provision-indicator
	              :sen-setting])

(def estab-type-to-estab-cat-csv->ds-opts
  "Options for `ds/->dataset` read of estab-type-to-estab-cat-csv CSV files."
  {:file-type   :csv
   :separator   ","
   :header-row? :true
   :key-fn      keyword
   :parser-fn   {:type-of-establishment-name    :string
	         :sen-unit-indicator            :boolean
	         :resourced-provision-indicator :boolean
	         :sen-setting                   :string
	         :estab-cat                     :string}})

(defn estab-type-to-estab-cat-ds
  "Dataset mapping Estab Types to `:estab-cat`s.
   Extracted/derived from `::estab-type-to-estab-cat` key.
   (Estab Type = GIAS Establishment Types split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [estab-type-to-estab-cat]}]
  (let [ds-template (tc/dataset nil {:dataset-name "estab-type-to-estab-cat"
                                     :column-names ((comp keys :parser-fn) estab-type-to-estab-cat-csv->ds-opts)})]
    (cond
      (string? estab-type-to-estab-cat)     (resource-or-file->dataset estab-type-to-estab-cat estab-type-to-estab-cat-csv->ds-opts)
      (tc/dataset? estab-type-to-estab-cat) estab-type-to-estab-cat
      (map? estab-type-to-estab-cat)        (tc/concat-copying ds-template (map->ds estab-type-to-estab-cat :vals-col-name :estab-cat))
      :else                                 ds-template)))

(defn estab-type-to-estab-cat
  "Map Estab Types to `:estab-cat`s.
   Extracted/derived from `::estab-type-to-estab-cat` key.
   (Estab Type = GIAS Establishment Types split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {::keys [estab-type-to-estab-cat]}]
  (as-> estab-type-to-estab-cat $
    (if (string? $)     (resource-or-file->dataset $ estab-type-to-estab-cat-csv->ds-opts) $)
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
(defn parse-cfg
  "Parse settings configuration map `cfg` replacing string filepath
   values of selected keys with the dataset read from the file."
  [& {::keys [edubaseall-send-map]
      :as    cfg}]
  (merge
   (let [parse-string-v (fn [k v f] (if (string? v) (f k v) v))]
     (reduce-kv
      (fn [m k v]
        (assoc m k (case k
                     ::estab-cats                   (parse-string-v k v estab-cats-ds)
                     ::designations                 (parse-string-v k v designations-ds)
                     ::areas                        (parse-string-v k v areas-ds)
                     ::sen2-estab-settings-manual   (parse-string-v k v sen2-estab-settings-manual-ds)
                     ::sen2-estab-settings-override (parse-string-v k v sen2-estab-settings-override-ds)
                     ::estab-type-to-estab-cat      (parse-string-v k v estab-type-to-estab-cat-ds)
                     v)))
      {}
      cfg))
   (when (nil? edubaseall-send-map) {::edubaseall-send-map (gias/edubaseall-send->map cfg)})))

(def standard-cfg-files
  "Configuration keys for standard settings CSV files."
  {::estab-cats                   "resources/standard/estab-cats.csv"
   ::designations                 "resources/standard/designations.csv"
   ::areas                        "resources/standard/areas.csv"
   ::sen2-estab-settings-manual   "resources/standard/sen2-estab-settings-manual.csv"
   ::sen2-estab-settings-override "resources/standard/sen2-estab-settings-override.csv"
   ::estab-type-to-estab-cat      "resources/standard/estab-type-to-estab-cat.csv"
   ::designation-f                standard-designation
   ::area-split-f                 standard-area-split})

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
              designation-f            (fn [& args] nil)
              area-split-f             (fn [& args] nil)
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
                              (some #(= ukprn (:ukprn %)) (vals edubaseall-send-map))
                              :else
                              nil)
        estab-name-via-gias (let [establishment-name (:establishment-name edubaseall-send)]
                              (when establishment-name
                                (str establishment-name
                                     (when sen-unit-indicator (str " " sen-unit-name))
                                     (when resourced-provision-indicator (str " " resourced-provision-name)))))
        ;; Derive estab-type from GIAS and sen2-estab info. Note this handles sen-setting too.
        estab-type          {:type-of-establishment-name    (:type-of-establishment-name edubaseall-send)
                             :sen-unit-indicator            sen-unit-indicator
                             :resourced-provision-indicator resourced-provision-indicator
                             :sen-setting                   sen-setting}
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
        setting             (string/join "_" (filter some? [estab-cat designation area]))]
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

