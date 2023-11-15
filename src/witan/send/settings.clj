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
  "Given map with maps as values `m`, returns dataset
   with keys and vals of vals in columns
   named per the keys prefixed with `key-prefix`."
  ;; TODO: Expand to unpack keys that are maps?
  [m key-prefix]
  (->> m
       (reduce-kv (fn [vec-of-rows-as-maps k v]
                    (conj vec-of-rows-as-maps
                          (merge {(keyword key-prefix) k}
                                 (update-keys v (comp keyword (partial str key-prefix "-") name)))))
                  [])
       tc/dataset))



;;; # Setting Definitions
(defn- filepath
  "Parse `filename`, `dir` & `resource-dir` parameters specifying definitions file to read:
   - if `filename` is specified with a path (i.e. contains a \"/\"), then use that filepath, otherwise
   - if `dir` is specified (non-nil), then prepend to `filename` to get filepath, otherwise
   - if `resource-dir` is specified (non-nil), then prepend to `filename` and open resource-dir file on that path,
   - else returns nil."
  [filename dir resource-dir]
  (cond
    (re-find #"\/" filename) filename
    (some? dir)              (str dir filename)
    (some? resource-dir)     (io/resource (str resource-dir filename))
    :else                    nil))

(def csv->ds-opts
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
  "Dataset of establishment category definitions.
   Read from CSV file specified by `::estab-cats-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::estab-cats-ds` val."
  [& {estab-cats-ds' ::estab-cats-ds
      ::keys         [estab-cats-filename
                      dir
                      resource-dir]
      :or            {estab-cats-filename "estab-cats.csv"}}]
  (or estab-cats-ds'
      (let [filepath (filepath estab-cats-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in (merge-with merge
                                         csv->ds-opts
                                         {:parser-fn {:designate?  :boolean
                                                      :split-area? :boolean}})))))
      (tc/dataset)))

(defn estab-cats
  "Map establishment category abbreviations to attributes.
   Derived from `(estab-cats-ds cfg)` unless specified in truthy `::estab-cats` val."
  [& {estab-cats' ::estab-cats
      :as         cfg}]
  (or estab-cats'
      (-> (estab-cats-ds cfg)
          (ds->sorted-map-by :abbreviation {:order-col :order}))))


;;; ## `:designation`: Designations
(defn designations-ds
  "Dataset of designation definitions.
   Read from CSV file specified by `::designations-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::designations-ds` val."
  [& {designations-ds' ::designations-ds
      ::keys           [designations-filename
                        dir
                        resource-dir]
      :or              {designations-filename "designations.csv"}}]
  (or designations-ds'
      (let [filepath (filepath designations-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in csv->ds-opts))))
      (tc/dataset)))

(defn designations
  "Map designation abbreviations to attributes.
   Derived from `(designations-ds cfg)` unless specified in truthy `::designations` val."
  [& {designations' ::designations
      :as           cfg}]
  (or designations'
      (-> (designations-ds cfg)
          (ds->sorted-map-by :abbreviation {:order-col :order}))))


;;; ## `:area`: Area
(defn areas-ds
  "Dataset of area definitions.
   Read from CSV file specified by `::areas-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::areas-ds` val."
  [& {areas-ds' ::areas-ds
      ::keys    [areas-filename
                 dir
                 resource-dir]
      :or       {areas-filename "areas.csv"}}]
  (or areas-ds'
      (let [filepath (filepath areas-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in csv->ds-opts))))
      (tc/dataset)))

(defn areas
  "Map area abbreviations to attributes.
  Derived from `(areas-ds cfg)` unless specified in truthy `::areas` val."
  [& {areas' ::areas
      :as    cfg}]
  (or areas'
      (-> (areas-ds cfg)
          (ds->sorted-map-by :abbreviation {:order-col :order}))))


;;; ## Settings
;; Combining `:estab-cat`, `:designation` and `:area`:
(defn settings
  "Map setting abbreviations to attributes.
   Derived from estab-cat, designation & area definitions unless specified in truthy `::settings` val:
   - estab-cat definitions from `(estab-cats cfg)`
   - designation definitions from `(designations cfg)`
   - area definitions from `(areas cfg)`"
  [& {settings' ::settings
      :as       cfg}]
  (or settings'
      (let [;; Derive definition datasets from maps in case maps have been provided in `cfg`
            ;; TODO: If maps not provided then use ds directly? Move map->ds to the ds fns if map provided?
            estab-cats'      (estab-cats cfg)
            designations'    (designations cfg)
            areas'           (areas cfg)
            estab-cats-ds'   (map->ds estab-cats' "estab-cat")
            designations-ds' (map->ds designations' "designation")
            areas-ds'        (map->ds areas' "area")]
        ;; TODO: Ensure works when not designating or area-splitting.
        ;; TODO: Only construct name|label|definition if have all components needed (i.e. need area if :estab-cat-split-area).
        (-> estab-cats-ds'
            ;; Expand `:estab-cat`s to be designated by designations
            (as-> $
                (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-designate?))
                                   (-> (tc/select-rows $ (comp :estab-cat-designate?))
                                       (tc/cross-join designations-ds'))))
            ;; Expand out `:estab-cat`s to be split by area
            (as-> $
                (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-split-area?))
                                   (-> (tc/select-rows $ (comp :estab-cat-split-area?))
                                       (tc/cross-join areas-ds'))))
            ;; Derive setting `:abbreviation`
            (tc/map-columns :abbreviation [:estab-cat :designation :area]
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
                                      {:name       (setting-attr-f estab-cat-name       designation-name       area-name      )})
                                    (when estab-cat-label
                                      {:label      (setting-attr-f estab-cat-label      designation-label      area-label     )})
                                    (when estab-cat-definition
                                      {:definition (setting-attr-f estab-cat-definition designation-definition area-definition)})))))
            ;; Tidy dataset
            (tc/select-columns [:abbreviation :order :name :label :definition :estab-cat :designation :area])
            ;; Convert to map
            (ds->sorted-map-by :abbreviation {:order-col :order})))))

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



;;; # Settings
(def sen2-estab-keys
  "SEN2 establishment column keywords from `placement-detail` table"
  [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting])


;;; ## Manual and Override settings
(defn sen2-estab-settings-manual-ds
  "Dataset mapping SEN2 Estab keys to manual settings.
   Read from CSV file specified by `::sen2-estab-settings-manual-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::sen2-estab-settings-manual-ds` val.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {sen2-estab-settings-manual-ds' ::sen2-estab-settings-manual-ds
      ::keys                        [sen2-estab-settings-manual-filename
                                     dir
                                     resource-dir]
      :or                           {sen2-estab-settings-manual-filename "sen2-estab-settings-manual.csv"}}]
  (or sen2-estab-settings-manual-ds'
      (let [filepath (filepath sen2-estab-settings-manual-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in {:file-type   :csv
                              :separator   ","
                              :header-row? :true
                              :column-blocklist ["reference-website"
                                                 "notes"]
                              :key-fn      keyword
                              :parser-fn   {:urn                           :string
                                            :ukprn                         :string
	                                    :sen-unit-indicator            :boolean
	                                    :resourced-provision-indicator :boolean
	                                    :sen-setting                   :string
	                                    :estab-cat                     :string
                                            :designation                   :string
                                            :la-code                       :string}}))))
      (tc/dataset)))

(defn sen2-estab-settings-manual
  "Map SEN2 Estab keys to manual settings.
   Derived from `(sen2-estab-settings-manual-ds cfg)` unless specified in truthy `::sen2-estab-settings-manual` val."
  [& {sen2-estab-settings-manual' ::sen2-estab-settings-manual
      :as                      cfg}]
  (or sen2-estab-settings-manual'
      (-> (sen2-estab-settings-manual-ds cfg)
          (ds->hash-map sen2-estab-keys))))

(defn sen2-estab-settings-override-ds
  "Dataset mapping SEN2 Estab keys to override settings.
   Read from CSV file specified by `::sen2-estab-settings-override-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::sen2-estab-settings-override-ds` val.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {sen2-estab-settings-override-ds' ::sen2-estab-settings-override-ds
      ::keys                        [sen2-estab-settings-override-filename
                                     dir
                                     resource-dir]
      :or                           {sen2-estab-settings-override-filename "sen2-estab-settings-override.csv"}}]
  (or sen2-estab-settings-override-ds'
      (let [filepath (filepath sen2-estab-settings-override-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in {:file-type   :csv
                              :separator   ","
                              :header-row? :true
                              :column-blocklist ["reference-website"
                                                 "notes"]
                              :key-fn      keyword
                              :parser-fn   {:urn                           :string
                                            :ukprn                         :string
	                                    :sen-unit-indicator            :boolean
	                                    :resourced-provision-indicator :boolean
	                                    :sen-setting                   :string
	                                    :estab-cat                     :string
                                            :designation                   :string
                                            :la-code                       :string}}))))
      (tc/dataset)))

(defn sen2-estab-settings-override
  "Map SEN2 Estab keys to override settings.
   Derived from `(sen2-estab-settings-override-ds cfg)` unless specified in truthy `::sen2-estab-settings-override` val."
  [& {sen2-estab-settings-override' ::sen2-estab-settings-override
      :as                      cfg}]
  (or sen2-estab-settings-override'
      (-> (sen2-estab-settings-override-ds cfg)
          (ds->hash-map sen2-estab-keys))))


;;; ## GIAS Establishment Type to `:estab-cat`
(def estab-type-keys [:type-of-establishment-name
                      :sen-unit-indicator
	              :resourced-provision-indicator
	              :sen-setting])

(defn estab-type-to-estab-cat-ds
  "Dataset mapping Estab Types to `:estab-cat` Estab Categories categories.
   Read from CSV file specified by `::estab-type-to-estab-cat-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::estab-type-to-estab-cat-ds` val.
  (Estab Type = GIAS Establishment Types split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {estab-type-to-estab-cat-ds' ::estab-type-to-estab-cat-ds
      ::keys                      [estab-type-to-estab-cat-filename
                                   dir
                                   resource-dir]
      :or                         {estab-type-to-estab-cat-filename "estab-type-to-estab-cat.csv"}}]
  (or estab-type-to-estab-cat-ds'
      (let [filepath (filepath estab-type-to-estab-cat-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in {:file-type   :csv
                              :separator   ","
                              :header-row? :true
                              :key-fn      keyword
                              :parser-fn   {:type-of-establishment-name    :string
	                                    :sen-unit-indicator            :boolean
	                                    :resourced-provision-indicator :boolean
	                                    :sen-setting                   :string
	                                    :estab-cat                     :string}}))))
      (tc/dataset)))

(defn estab-type-to-estab-cat
  "Map Estab Types to `:estab-cat`s.
   Derived from `(estab-type-to-estab-cat-ds cfg)` unless specified in truthy `::estab-type-to-estab-cat` val."
  [& {estab-type-to-estab-cat' ::estab-type-to-estab-cat
      :as                      cfg}]
  (or estab-type-to-estab-cat'
      (-> (estab-type-to-estab-cat-ds cfg)
          (ds->hash-map estab-type-keys)
          (update-vals :estab-cat))))


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

(defn standard-designation-f
  [& {:keys [sen-provision-types-vec]}]
  (sen-provision-types-vec->designation sen-provision-types-vec))


;;; ## Area derivation
(defn area-split-for-la-code
  "Returns \"InA\" if `la-code` is in set `in-area-la-codes`, \"OoA\" if not and not nil, otherwise \"XxX\"."
  [in-area-la-codes la-code]
  (cond (in-area-la-codes la-code) "InA"
        (some? la-code)            "OoA"
        :else                      "XxX"))

(defn standard-area-split-f
  [& {:keys  [la-code]
      ::keys [in-area-la-codes]}]
  (area-split-for-la-code in-area-la-codes la-code))


;;; ## Setting for sen2-estab
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
   & {::keys [designation-f
              area-split-f
              in-area-la-codes
              sen-unit-name
              resourced-provision-name
              edubaseall-send-map]
      :or    {designation-f            (fn [& args] nil)
              area-split-f             (fn [& args] nil)
              sen-unit-name            "(SEN Unit)"
              resourced-provision-name "(Resourced Provision)"}
      :as    cfg}]
  (let [sen2-estab          {:urn                           urn
                             :ukprn                         ukprn
                             :sen-unit-indicator            sen-unit-indicator
                             :resourced-provision-indicator resourced-provision-indicator
                             :sen-setting                   sen-setting}
        ;; Get any override or manual setting information for this sen2-estab
        override            (get (sen2-estab-settings-override cfg) sen2-estab)
        manual              (get (sen2-estab-settings-manual cfg) sen2-estab)
        ;; Get GIAS information for this sen2-estab
        edubaseall-send     ((or edubaseall-send-map
                                 (gias/edubaseall-send->map cfg))
                             urn)
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
        estab-cat-via-gias  (get (estab-type-to-estab-cat cfg) estab-type)
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
        designate?          (get-in (estab-cats cfg) [estab-cat :designate?])
        designation         (when designate? (or (get override :designation)
                                                 (designation-f (assoc sen2-estab
                                                                       :estab-cat estab-cat
                                                                       :sen-provision-types-vec (get edubaseall-send :sen-provision-types-vec)
                                                                       :edubaseall-send edubaseall-send))
                                                 (get manual :designation)
                                                 (when (not-any? boolean (vals sen2-estab)) "UKN") ; Only possible if estab-cat "UKN" designated!?
                                                 "XxX")) ; Avoid "XxX" via manual, designation-f, or override.
        ;; Derive `:area` for `:estab-cat`s to be split by area:
        split-area?         (get-in (estab-cats cfg) [estab-cat :split-area?])
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

